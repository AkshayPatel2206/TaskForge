import "dotenv/config";
import prisma from "../lib/prisma.js";
import { deadLetterQueue, jobQueue } from "../lib/queue.js";

async function recoverDeadLetterJobs() {
  const jobs = await prisma.job.findMany({
    where: {
      status: "FAILED",
      deadLetteredAt: null,
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

  if (jobs.length === 0) {
    console.log("No jobs need dead-letter recovery.");
    return;
  }

  console.log(`Found ${jobs.length} jobs to recover.`);

  for (const job of jobs) {
    const latestAttempt = job.attempts[0];

    const errorMessage =
      job.deadLetterError ??
      latestAttempt?.errorMessage ??
      "Job failed after all retry attempts";

    try {
      await deadLetterQueue.add(
        "dead-letter-job",
        {
          jobId: job.id,
          errorMessage,
          failedAt: (
            latestAttempt?.finishedAt ?? job.updatedAt
          ).toISOString(),
          attemptsMade: latestAttempt?.attemptNumber ?? 0,
        },
        {
          jobId: `${job.id}-dead-letter`,
        },
      );

      await prisma.job.update({
        where: { id: job.id },
        data: {
          deadLetteredAt: new Date(),
          deadLetterError: null,
        },
      });

      console.log(`Recovered dead-letter delivery for job ${job.id}`);
    } catch (error) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : "Unknown dead-letter recovery error";

      await prisma.job.update({
        where: { id: job.id },
        data: {
          deadLetterError: errorMessage,
        },
      });

      console.error(
        `Recovery failed for job ${job.id}:`,
        errorMessage,
      );
    }
  }
}

try {
  await recoverDeadLetterJobs();
} catch (error) {
  console.error("Dead-letter recovery script failed:", error);
  process.exitCode = 1;
} finally {
  await Promise.all([
    deadLetterQueue.close(),
    jobQueue.close(),
    prisma.$disconnect(),
  ]);
}