import "dotenv/config";
import { Worker } from "bullmq";
import prisma from "../lib/prisma.js";
import { deadLetterQueue } from "../lib/queue.js";
const worker = new Worker("taskforge-jobs", async (bullJob) => {
    const { jobId } = bullJob.data as { jobId: string };

    console.log(`Processing job: ${jobId}`);

    const job = await prisma.job.findUnique({
        where: { id: jobId },
    });

    if (!job) {
        throw new Error(`Job ${jobId} not found in PostgreSQL`);
    }

    if (job.status !== "PENDING") {
        console.log(`Skipping job ${jobId} with status ${job.status}`);
        return;
    }

    const updated = await prisma.job.updateMany({
        where: {
            id: jobId,
            status: "PENDING",
        },
        data: {
            status: "RUNNING"
        }
    })

    if (updated.count === 0) {
        console.log(`Job ${jobId} was already claimed or changed`);
        return;
    }

    const prevAttemps = await prisma.jobAttempt.count({
        where: { jobId },
    })

    const attemptNumber = prevAttemps + 1;

    const attemp = await prisma.jobAttempt.create({
        data: {
            jobId,
            attemptNumber,
            status: "RUNNING",
            startedAt: new Date(),
        }
    })


    try {
        await new Promise((resolve) => setTimeout(resolve, 3000));
        //throw new Error("Intentional retry test failure");
        //throw new Error("Intentional DLQ test failure");


        await prisma.$transaction([
            prisma.jobAttempt.update({
                where: {
                    id: attemp.id,
                },
                data: {
                    status: "SUCCEEDED",
                    finishedAt: new Date(),
                },
            }),

            prisma.job.update({
                where: { id: jobId },
                data: {
                    status: "COMPLETED",
                    result: { message: "Job processed successfully" },
                },
            }),
        ]);
        console.log(`Job ${jobId} completed successfully`);


    } catch (error) {

        const errorMessage =
            error instanceof Error ? error.message : "Unknown processing error";


        const maxAttempts = bullJob.opts.attempts ?? 1;
        const isLastAttempt = bullJob.attemptsMade + 1 >= maxAttempts;

        await prisma.$transaction([
            prisma.jobAttempt.update({
                where: {
                    id: attemp.id,
                },
                data: {
                    status: "FAILED",
                    errorMessage,
                    finishedAt: new Date(),
                }
            }),
            prisma.job.update({
                where: { id: jobId },
                data: {
                    status: isLastAttempt ? "FAILED" : "PENDING"
                },
            })
        ]);

        console.log(`Job ${jobId} failed on attempt ${bullJob.attemptsMade + 1} / ${maxAttempts}`);

        if (isLastAttempt) {
            try {
                await deadLetterQueue.add("dead-letter-job", {
                    jobId,
                    errorMessage,
                    failedAt: new Date().toISOString(),
                    attemptsMade: bullJob.attemptsMade + 1,
                }, {
                    jobId: `${jobId}-dead-letter`,
                });

                await prisma.job.update({
                    where: { id: jobId },
                    data: {
                        deadLetteredAt: new Date(),
                        deadLetterError: null,
                    },
                });
                console.log(`Job ${jobId} moved to dead-letter queue`);
            } catch (error) {
                const dlqErrorMessage =
                    error instanceof Error
                        ? error.message
                        : "Unknown DLQ delivery error";

                await prisma.job.update({
                    where: { id: jobId },
                    data: {
                        deadLetterError: dlqErrorMessage,
                    },
                });

                console.error(
                    `Failed to enqueue job ${jobId} in dead-letter queue:`,
                    dlqErrorMessage,
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
        }
    })

worker.on("completed", (job) => {
    console.log(`Queue job ${job.id} compleated`);
});

worker.on("failed", (job, error) => {
    console.error(`Queue job ${job?.id} failed:`, error.message);
});

console.log("TaskForge worker is running");