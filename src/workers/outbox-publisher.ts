
import "dotenv/config";
import logger from "../lib/logger.js";
import prisma from "../lib/prisma.js";
import { jobQueue } from "../lib/queue.js";
import { Redis } from "ioredis";

const POLL_INTERVAL = 5000;

// Outbox publisher heartbeat
const heartbeatRedis = new Redis({
    host: process.env.REDIS_HOST || "localhost",
    port: Number(process.env.REDIS_PORT || 6379),
});

heartbeatRedis.on("error", (error) => {
    logger.error(
        { err: error },
        "Outbox heartbeat Redis error"
    );
});

const HEARTBEAT_INTERVAL = 10_000;
const HEARTBEAT_TTL = 30;

async function updatePublisherHeartbeat() {
    try {
        await heartbeatRedis.set(
            "taskforge:health:outbox-publisher",
            new Date().toISOString(),
            "EX",
            HEARTBEAT_TTL
        );
    } catch (error) {
        logger.error(
            { err: error },
            "Failed to update outbox publisher heartbeat"
        );
    }
}

// Send first heartbeat immediately
void updatePublisherHeartbeat();

// Refresh heartbeat every 10 seconds
const heartbeatTimer = setInterval(() => {
    void updatePublisherHeartbeat();
}, HEARTBEAT_INTERVAL);

let isRunning = true;

logger.info("Outbox publisher file is executing...");

// Recover stale outbox events
async function recoverStaleEvents() {
    const staleTime = new Date(Date.now() - 60 * 1000);

    const result = await prisma.outboxEvent.updateMany({
        where: {
            status: "PROCESSING",
            claimedAt: {
                lt: staleTime,
            },
        },
        data: {
            status: "PENDING",
            claimedAt: null,
            retryCount: {
                increment: 1,
            },
            lastError: "Publisher timed out. Event moved back to PENDING.",
        },
    });

    if (result.count > 0) {
        logger.warn(
            { recoveredCount: result.count },
            "Recovered stale outbox events"
        );
    }
}

// Publish pending outbox events to BullMQ
async function publishPendingEvents() {
    const events = await prisma.outboxEvent.findMany({
        where: {
            status: "PENDING",
        },
        orderBy: {
            createdAt: "asc",
        },
        take: 10,
    });

    for (const event of events) {
        const payload = event.payload as {
            jobId?: string;
            correlationId?: string;
        };

        const jobId = payload.jobId;
        const correlationId = payload.correlationId;

        const logContext = {
            eventId: event.id,
            jobId,
            correlationId,
        };

        try {
            // Claim event atomically
            const claim = await prisma.outboxEvent.updateMany({
                where: {
                    id: event.id,
                    status: "PENDING",
                },
                data: {
                    status: "PROCESSING",
                    claimedAt: new Date(),
                },
            });

            if (claim.count === 0) {
                logger.warn(
                    logContext,
                    "Outbox event already claimed"
                );
                continue;
            }

            if (!jobId) {
                throw new Error("Outbox event does not contain jobId.");
            }

            // Add job to Redis / BullMQ
            await jobQueue.add(
                "process-job",
                { jobId, correlationId },
                {
                    jobId,
                    attempts: 3,
                    backoff: {
                        type: "exponential",
                        delay: 1000,
                    },
                }
            );

            // Mark event as published
            await prisma.outboxEvent.update({
                where: {
                    id: event.id,
                },
                data: {
                    status: "PUBLISHED",
                    publishedAt: new Date(),
                    claimedAt: null,
                    lastError: null,
                },
            });

            logger.info(
                logContext,
                "Outbox event published successfully"
            );

        } catch (error) {
            const errorMessage =
                error instanceof Error
                    ? error.message
                    : "Unknown publishing error";

            logger.error(
                { ...logContext, err: error },
                "Failed to publish outbox event"
            );

            // Keep event PENDING so it can be retried
            await prisma.outboxEvent.update({
                where: {
                    id: event.id,
                },
                data: {
                    status: "PENDING",
                    claimedAt: null,
                    retryCount: {
                        increment: 1,
                    },
                    lastError: errorMessage,
                },
            }).catch((dbError) => {
                logger.error(
                    { ...logContext, err: dbError },
                    "Failed to update outbox error"
                );
            });
        }
    }
}

// Start publisher polling
async function startOutboxPublisher() {
    logger.info("Outbox publisher started...");

    while (isRunning) {
        try {
            await recoverStaleEvents();
            await publishPendingEvents();
        } catch (error) {
            logger.error(
                { err: error },
                "Outbox polling failed"
            );
        }

        if (isRunning) {
            await new Promise((resolve) =>
                setTimeout(resolve, POLL_INTERVAL)
            );
        }
    }
}

// Graceful shutdown
async function shutdown(signal: string) {
    logger.info(
        `${signal} received. Shutting down outbox publisher...`
    );

    isRunning = false;
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

try {
    await startOutboxPublisher();
} catch (error) {
    logger.error(
        { err: error },
        "Outbox publisher stopped unexpectedly"
    );
    process.exitCode = 1;
} finally {
    clearInterval(heartbeatTimer);

    try {
        await heartbeatRedis.del(
            "taskforge:health:outbox-publisher"
        );

        logger.info("Outbox publisher heartbeat removed.");
    } catch (error) {
        logger.error(
            { err: error },
            "Failed to remove outbox publisher heartbeat"
        );
    }

    await Promise.all([
        jobQueue.close(),
        prisma.$disconnect(),
        heartbeatRedis.quit(),
    ]);

    logger.info("Outbox publisher stopped.");
}