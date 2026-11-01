
import "dotenv/config";
import prisma from "../lib/prisma.js";
import { jobQueue } from "../lib/queue.js";

const STUCK_TIMEOUT_MINUTES = 5;

const RECOVERY_ERROR =
    "Worker stopped unexpectedly. Job recovered.";

async function enqueueRecoveredJob(
    jobId: string,
    attemptId: string
) {
    await jobQueue.add(
        "process-job",
        { jobId },
        {
            // Same attempt always gets the same queue ID.
            jobId: `recovery-${attemptId}`,
            attempts: 3,
            backoff: {
                type: "exponential",
                delay: 1000,
            },
        }
    );
}

async function recoverStuckJobs() {
    const cutoffTime = new Date(
        Date.now() - STUCK_TIMEOUT_MINUTES * 60 * 1000
    );

    console.log("Checking PostgreSQL for RUNNING jobs...");

    const runningJobs = await prisma.job.findMany({
        where: {
            status: "RUNNING",
        },
        include: {
            attempts: {
                orderBy: {
                    attemptNumber: "desc",
                },
                take: 1,
            },
        },
    });

    console.log(`Total RUNNING jobs found: ${runningJobs.length}`);

    const stuckJobs = runningJobs.filter((job) => {
        const latestAttempt = job.attempts[0];

        return (
            latestAttempt?.status === "RUNNING" &&
            latestAttempt.startedAt < cutoffTime
        );
    });

    console.log(`Total stuck jobs found: ${stuckJobs.length}`);

    for (const job of stuckJobs) {
        const latestAttempt = job.attempts[0];

        if (!latestAttempt) continue;

        try {
            // Update the attempt and job together.
            const recovered = await prisma.$transaction(async (tx) => {
                const attemptUpdate = await tx.jobAttempt.updateMany({
                    where: {
                        id: latestAttempt.id,
                        status: "RUNNING",
                        startedAt: {
                            lt: cutoffTime,
                        },
                    },
                    data: {
                        status: "FAILED",
                        errorMessage: RECOVERY_ERROR,
                        finishedAt: new Date(),
                    },
                });

                if (attemptUpdate.count === 0) {
                    return false;
                }

                const jobUpdate = await tx.job.updateMany({
                    where: {
                        id: job.id,
                        status: "RUNNING",
                    },
                    data: {
                        status: "PENDING",
                    },
                });

                if (jobUpdate.count === 0) {
                    throw new Error("Job status changed during recovery.");
                }

                return true;
            });

            if (!recovered) {
                console.log(`Job ${job.id} was already changed.`);
                continue;
            }

            await enqueueRecoveredJob(job.id, latestAttempt.id);

            console.log(`Recovered stuck job: ${job.id}`);
        } catch (error) {
            console.error(`Failed to recover job ${job.id}:`, error);
        }
    }

    // Retry enqueueing recovered jobs if a previous Redis operation failed.
    console.log("Checking for recovered jobs awaiting enqueue...");

    const pendingJobs = await prisma.job.findMany({
        where: {
            status: "PENDING",
            attempts: {
                some: {
                    status: "FAILED",
                    errorMessage: RECOVERY_ERROR,
                },
            },
        },
        include: {
            attempts: {
                orderBy: {
                    attemptNumber: "desc",
                },
                take: 1,
            },
        },
    });

    for (const job of pendingJobs) {
        const latestAttempt = job.attempts[0];

        if (
            !latestAttempt ||
            latestAttempt.status !== "FAILED" ||
            latestAttempt.errorMessage !== RECOVERY_ERROR
        ) {
            continue;
        }

        try {
            await enqueueRecoveredJob(job.id, latestAttempt.id);

            console.log(`Re-enqueued recovered job: ${job.id}`);
        } catch (error) {
            console.error(
                `Failed to re-enqueue job ${job.id}:`,
                error
            );
        }
    }
}

try {
    await recoverStuckJobs();
} catch (error) {
    console.error("Stuck-job recovery failed:", error);
    process.exitCode = 1;
} finally {
    await Promise.all([
        jobQueue.close(),
        prisma.$disconnect(),
    ]);
}
