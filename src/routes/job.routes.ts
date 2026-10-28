import { jobQueue } from "../lib/queue.js";
import { Router } from "express";
import prisma from "../lib/prisma.js";
import { isAuthenticated } from "../middleware/auth.middleware.js";
import { createJobSchema } from "../schemas/job.schema.js";
import { tryCatch } from "bullmq";

const router = Router();

router.use(isAuthenticated);

router.get("/", async (req, res) => {
  try {

    const page = Number(req.query.page ?? 1);
    const limit = Number(req.query.limit ?? 10);
    const status = req.query.status;

    if (
      !Number.isInteger(page) ||
      !Number.isInteger(limit) ||
      page < 1 ||
      limit < 1 ||
      limit > 100
    ) {
      res.status(400).json({
        success: false,
        message: "page must be >= 1 and limit must be between 1 and 100",
      });
      return;
    }

    const validStatuses = [
      "PENDING",
      "RUNNING",
      "COMPLETED",
      "FAILED",
      "CANCELLED",
    ];

    if (
      status !== undefined &&
      (typeof status !== "string" || !validStatuses.includes(status))
    ) {
      res.status(400).json({
        success: false,
        message: "Invalid job status",
      });
      return;
    }

    const where = {
      userId: req.user!.id,
      ...(typeof status === "string" ? { status: status as "PENDING" | "RUNNING" | "COMPLETED" | "FAILED" } : {})
    }

    const [jobs, totalJobs] = await prisma.$transaction([
      prisma.job.findMany({
        where,
        orderBy: {
          createdAt: "desc",
        },
        skip: (page - 1) * limit,
        take: limit,
      }),
      prisma.job.count({ where }),
    ])
    res.status(200).json({
      success: true,
      data: jobs,
      pagination: {
        page,
        limit,
        totalJobs,
        totalPages: Math.ceil(totalJobs / limit),
      },
    });
  } catch (error) {
    console.error("Failed to fetch jobs:", error);

    res.status(500).json({
      success: false,
      message: "Failed to fetch jobs",
    });
  }
});

router.get("/failed", async (req, res) => {
    try {
        const jobs = await prisma.job.findMany({
            where: {
                userId: req.user!.id,
                status: "FAILED",
            },
            include: {
                attempts: {
                    orderBy: {
                        attemptNumber: "asc",
                    },
                },
            },
            orderBy: {
                updatedAt: "desc",
            },
        });

        res.status(200).json({
            success: true,
            data: jobs,
        });
    } catch (error) {
        console.error("Failed to fetch failed jobs:", error);

        res.status(500).json({
            success: false,
            message: "Failed to fetch failed jobs",
        });
    }
});
router.get("/:id", async (req, res) => {
  try {
    const job = await prisma.job.findFirst({
      where: {
        id: req.params.id,
        userId: req.user!.id,
      },
      include:{
        attempts:{
          orderBy:{
            attemptNumber:"asc"
          }
        }
      }
    });

    if (!job) {
      res.status(404).json({
        success: false,
        message: "Job not found",
      });
      return;
    }

    res.status(200).json({
      success: true,
      data: job,
    });
  } catch (error) {
    console.error("Failed to fetch job:", error);

    res.status(500).json({
      success: false,
      message: "Failed to fetch job",
    });
  }
});

router.post("/", async (req, res) => {
  const validation = createJobSchema.safeParse(req.body);

  if (!validation.success) {
    res.status(400).json({
      success: false,
      errors: validation.error.issues,
    });
    return;
  }

  try {
    const { title, payload } = validation.data;

    const job = await prisma.job.create({
      data: {
        userId: req.user!.id,
        title,
        ...(payload !== undefined ? { payload: payload as object } : {}),
      },
    });

    try {
      await jobQueue.add(
        "process-job",
        { jobId: job.id },
        { jobId: job.id,
          attempts:3,
          backoff:{
            type:"exponential",
            delay:1000,
          }
         },
      )
    } catch (error) {
      console.error("Failed to enqueue job:", error);
      await prisma.job.deleteMany({
        where: {
          id: job.id,
          userId: req.user!.id,
          status: "PENDING",
        }
      })

      res.status(503).json({
        success: false,
        message: "Unable to queue job. Please try again.",
      });
      return;
    }

     res.status(201).json({
      success: true,
      data: job,
      message: "Job created and queued successfully",
    });

    

    
  } catch (error) {
    console.error("Failed to create job:", error);

    res.status(500).json({
      success: false,
      message: "Failed to create job",
    });
  }
});


router.delete("/:id", async (req, res) => {
  try {
    const job = await prisma.job.findFirst({
      where: {
        id: req.params.id,
        userId: req.user!.id,
      }
    });

    if (!job) {
      res.status(404).json({
        success: false,
        message: "Job not found",
      });
      return;
    }

    if (job.status !== "PENDING") {
      res.status(409).json({
        success: false,
        message: `Cannot delete a job with status ${job.status}`,
      });
      return;
    }

    const deleted = await prisma.job.deleteMany({
      where: {
        id: job.id,
        userId: req.user!.id,
        status: "PENDING",
      },

    })

    if (deleted.count === 0) {
      res.status(409).json({
        success: false,
        message: "Job status changed; try again",
      })
      return;
    }
    res.status(200).json({
      success: true,
      message: "Pending job deleted successfully",
    })


  } catch (error) {
    console.error("Failed to delete job:", error);

    res.status(500).json({
      success: false,
      message: "Failed to delete job",
    });
  }
})


router.post("/:id/cancel", async (req, res) => {
  try {
    const job = await prisma.job.findFirst({
      where: {
        id: req.params.id,
        userId: req.user!.id,
      },
    });

    if (!job) {
      res.status(404).json({
        success: false,
        message: "Job not found",
      });
      return;
    }

    if (job.status !== "PENDING") {
      res.status(409).json({
        success: false,
        message: `Cannot cancel a job with status ${job.status}`,
      });
      return;
    }

    const updated = await prisma.job.updateMany({
      where: {
        id: job.id,
        userId: req.user!.id,
        status: "PENDING",
      },
      data: {
        status: "CANCELLED",
      },
    });

    if (updated.count === 0) {
      res.status(409).json({
        success: false,
        message: "Job status changed; try again",
      });
      return;
    }

    res.status(200).json({
      success: true,
      message: "Job cancelled successfully",
    });
  } catch (error) {
    console.error("Failed to cancel job:", error);
    res.status(500).json({
      success: false,
      message: "Failed to cancel job",
    });
  }
});


router.post("/:id/retry", async (req, res) => {
  try {
    const job = await prisma.job.findFirst({
      where: {
        id: req.params.id,
        userId: req.user!.id,
      },
    });

    if (!job) {
      res.status(404).json({
        success: false,
        message: "Job not found",
      });
      return;
    }

    if (job.status !== "FAILED") {
      res.status(409).json({
        success: false,
        message: `Cannot retry a job with status ${job.status}`,
      });
      return;
    }

    // Change FAILED to PENDING only if it is still FAILED.
    const updated = await prisma.job.updateMany({
      where: {
        id: job.id,
        userId: req.user!.id,
        status: "FAILED",
      },
      data: {
        status: "PENDING",
        result: {},
      },
    });

    if (updated.count === 0) {
      res.status(409).json({
        success: false,
        message: "Job status changed; please try again",
      });
      return;
    }

    try {
      await jobQueue.add(
        "process-job",
        { jobId: job.id },
        {
          // Use a new queue ID for each manual retry.
          jobId: `${job.id}-manual-${Date.now()}`,
          attempts: 3,
          backoff: {
            type: "exponential",
            delay: 1000,
          },
        },
      );
    } catch (queueError) {
      console.error("Failed to enqueue retry:", queueError);

      await prisma.job.updateMany({
        where: {
          id: job.id,
          userId: req.user!.id,
          status: "PENDING",
        },
        data: {
          status: "FAILED",
        },
      });

      res.status(503).json({
        success: false,
        message: "Unable to queue retry. Please try again.",
      });
      return;
    }

    res.status(202).json({
      success: true,
      message: "Job retry queued successfully",
      data: {
        jobId: job.id,
        status: "PENDING",
      },
    });
  } catch (error) {
    console.error("Failed to retry job:", error);

    res.status(500).json({
      success: false,
      message: "Failed to retry job",
    });
  }
});




export default router;
