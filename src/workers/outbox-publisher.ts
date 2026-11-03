import "dotenv/config";
import prisma from "../lib/prisma.js";
import { jobQueue } from "../lib/queue.js";

const POLL_INTERVAL = 5000;

let isRunning = true;
console.log("Outbox publisher file is executing...");


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
    console.log(`Recovered ${result.count} stale outbox event(s).`);
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
        try {

            const claim = await prisma.outboxEvent.updateMany({
                where:{
                    id:event.id,
                    status:"PENDING",
                },
                data:{
                    status:"PROCESSING",
                    claimedAt:new Date(),
                }
            })

            if(claim.count === 0){
                console.log(`event already claimed ${event.id}`);
                continue;

            }
            const payload = event.payload as {
                jobId?: string;
            };
            const jobId = payload.jobId;

            if (!jobId) {
                throw new Error("Outbox event does not contain jobId.");
            }

            // Add the job to Redis / BullMQ
            await jobQueue.add(
                "process-job",
                { jobId },
                {
                    jobId: jobId,
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
                    claimedAt:null,
                    lastError: null,
                },
            });

            console.log(`Published event: ${event.id}`);
            console.log(`Queued job: ${jobId}`);

        } catch (error) {
            console.error(
                `Failed to publish event ${event.id}:`,
                error
            );

            // Keep event PENDING so it can be retried
            await prisma.outboxEvent.update({
                where: {
                    id: event.id,
                },
                data: {
                    status:"PENDING",
                    claimedAt:null,
                    retryCount: {
                        increment: 1,
                    },
                    lastError:
                        error instanceof Error
                            ? error.message
                            : "Unknown publishing error",
                },
            }).catch((dbError) => {
                console.error(
                    "Failed to update outbox error:",
                    dbError
                );
            });
        }
    }
}

async function startOutboxPublisher() {
    console.log("Outbox publisher started...");

    while (isRunning) {
        try {
            await publishPendingEvents();
        } catch (error) {
            console.error("Outbox polling failed:", error);
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
    console.log(`${signal} received. Shutting down outbox publisher...`);

    isRunning = false;
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

try {
    await recoverStaleEvents();
    await startOutboxPublisher();
} catch (error) {
    console.error("Outbox publisher stopped unexpectedly:", error);
    process.exitCode = 1;
} finally {
    await Promise.all([
        jobQueue.close(),
        prisma.$disconnect(),
    ]);

    console.log("Outbox publisher stopped.");
}