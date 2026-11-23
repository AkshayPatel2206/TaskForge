
import "dotenv/config";
import logger from "../lib/logger.js";
import { Worker } from "bullmq";
import prisma from "../lib/prisma.js";
import { deadLetterQueue } from "../lib/queue.js";
import { Redis } from "ioredis";


// Worker heartbeat connection
const heartbeatRedis = new Redis({
    host: process.env.REDIS_HOST || "localhost",
    port: Number(process.env.REDIS_PORT || 6379),
});

heartbeatRedis.on("error", (error) => {
    logger.error({ err: error }, "Worker heartbeat Redis error");
});

// Update heartbeat every 10 seconds
const HEARTBEAT_INTERVAL = 10_000;
const HEARTBEAT_TTL = 30;

async function updateWorkerHeartbeat() {
    try {
        await heartbeatRedis.set(
            "taskforge:health:worker",
            new Date().toISOString(),
            "EX",
            HEARTBEAT_TTL
        );
    } catch (error) {
        logger.error(
            { err: error },
            "Failed to update worker heartbeat"
        );
    }
}

// Send first heartbeat immediately
void updateWorkerHeartbeat();

// Continue sending heartbeat every 10 seconds
const heartbeatTimer = setInterval(() => {
    void updateWorkerHeartbeat();
}, HEARTBEAT_INTERVAL);


const worker = new Worker(
    "taskforge-jobs",
    async (bullJob) => {
        const { jobId, correlationId } = bullJob.data as {
            jobId: string;
            correlationId?: string;
        };

        const logContext = { jobId, correlationId };

        logger.info(logContext, "Worker started processing job");

        const job = await prisma.job.findUnique({
            where: { id: jobId },
        });

        if (!job) {
            throw new Error(`Job ${jobId} not found in PostgreSQL`);
        }

        if (job.status !== "PENDING") {
            logger.warn(
                { ...logContext, status: job.status },
                "Skipping job because it is not pending"
            );
            return;
        }

        const updated = await prisma.job.updateMany({
            where: {
                id: jobId,
                status: "PENDING",
            },
            data: {
                status: "RUNNING",
            },
        });

        if (updated.count === 0) {
            logger.warn(logContext, "Job was already claimed or changed");
            return;
        }

        const prevAttempts = await prisma.jobAttempt.count({
            where: { jobId },
        });

        const attemptNumber = prevAttempts + 1;

        const attempt = await prisma.jobAttempt.create({
            data: {
                jobId,
                attemptNumber,
                status: "RUNNING",
                startedAt: new Date(),
            },
        });

        logger.info(
            { ...logContext, attemptNumber },
            "Job attempt started"
        );

        try {
            await new Promise((resolve) => setTimeout(resolve, 3000));

            await prisma.$transaction([
                prisma.jobAttempt.update({
                    where: { id: attempt.id },
                    data: {
                        status: "SUCCEEDED",
                        finishedAt: new Date(),
                    },
                }),

                prisma.job.update({
                    where: { id: jobId },
                    data: {
                        status: "COMPLETED",
                        result: {
                            message: "Job processed successfully",
                        },
                    },
                }),
            ]);

            logger.info(
                { ...logContext, attemptNumber },
                "Job completed successfully"
            );

        } catch (error) {
            const errorMessage =
                error instanceof Error
                    ? error.message
                    : "Unknown processing error";

            const maxAttempts = bullJob.opts.attempts ?? 1;
            const isLastAttempt =
                bullJob.attemptsMade + 1 >= maxAttempts;

            await prisma.$transaction([
                prisma.jobAttempt.update({
                    where: { id: attempt.id },
                    data: {
                        status: "FAILED",
                        errorMessage,
                        finishedAt: new Date(),
                    },
                }),

                prisma.job.update({
                    where: { id: jobId },
                    data: {
                        status: isLastAttempt ? "FAILED" : "PENDING",
                    },
                }),
            ]);

            logger.error(
                {
                    ...logContext,
                    err: error,
                    attemptNumber: bullJob.attemptsMade + 1,
                    maxAttempts,
                },
                "Job processing attempt failed"
            );

            if (isLastAttempt) {
                try {
                    await deadLetterQueue.add(
                        "dead-letter-job",
                        {
                            jobId,
                            correlationId,
                            errorMessage,
                            failedAt: new Date().toISOString(),
                            attemptsMade: bullJob.attemptsMade + 1,
                        },
                        {
                            jobId: `${jobId}-dead-letter`,
                        }
                    );

                    await prisma.job.update({
                        where: { id: jobId },
                        data: {
                            deadLetteredAt: new Date(),
                            deadLetterError: null,
                        },
                    });

                    logger.warn(
                        { ...logContext, errorMessage },
                        "Job moved to dead-letter queue"
                    );

                } catch (dlqError) {
                    const dlqErrorMessage =
                        dlqError instanceof Error
                            ? dlqError.message
                            : "Unknown DLQ delivery error";

                    await prisma.job.update({
                        where: { id: jobId },
                        data: {
                            deadLetterError: dlqErrorMessage,
                        },
                    });

                    logger.error(
                        { ...logContext, err: dlqError },
                        "Failed to enqueue job in dead-letter queue"
                    );
                }
            }

            throw error;
        }
    },
    {
        connection: {
            host: process.env.REDIS_HOST || "localhost",
            port: Number(process.env.REDIS_PORT || 6379),
        },
        concurrency: 5,
    }
);

// BullMQ events

worker.on("completed", (job) => {
    logger.info(
        {
            jobId: job.data.jobId,
            correlationId: job.data.correlationId,
            queueJobId: job.id,
        },
        "Queue job completed"
    );
});

worker.on("failed", (job, error) => {
    logger.error(
        {
            jobId: job?.data.jobId,
            correlationId: job?.data.correlationId,
            queueJobId: job?.id,
            err: error,
        },
        "Queue job failed"
    );
});

worker.on("error", (error) => {
    logger.error({ err: error }, "Worker encountered an error");
});

logger.info("TaskForge worker is running");

// Graceful shutdown

async function gracefulShutdown(signal: string) {
    logger.info(`${signal} received. Shutting down worker...`);

    try {
        await worker.close();
        logger.info("Worker closed successfully.");

        clearInterval(heartbeatTimer);

        await heartbeatRedis.del("taskforge:health:worker");
        await heartbeatRedis.quit();

        logger.info("Worker heartbeat stopped successfully.");

        await prisma.$disconnect();
        logger.info("Prisma disconnected successfully.");

        process.exit(0);
    } catch (error) {
        logger.error({ err: error }, "Error during graceful shutdown");
        process.exit(1);
    }
}

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
