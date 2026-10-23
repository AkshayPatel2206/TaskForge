import express from "express";
import dotenv from "dotenv";
import prisma from "./lib/prisma.js";
import aurhRoutes from "./routes/auth.routes.js";
import { createJobSchema } from "./schemas/job.schema.js"
import { isAuthenticated } from "./middleware/auth.middleware.js";
dotenv.config();

const app = express();
const PORT = process.env.PORT || 4000;

app.use(express.json());
app.use("/api/auth", aurhRoutes);

app.get("/health", (_req, res) => {
    res.json({
        status: "ok",
        service: "TaskForge Api"
    })
})

app.get("/api/jobs", isAuthenticated, async (req, res) => {
    try {
        const jobs = await prisma.job.findMany({
            where: {
                userId: req.user!.id,
            },
            orderBy: {
                createdAt: "desc",
            }
        });

        res.status(200).json({
            success: true,
            count: jobs.length,
            data: jobs,
        });


    } catch (error) {
        console.log("Failed to fetch jobs:", error);

        res.status(500).json({
            success: false,
            message: "Failed to fetch jobs",
        });
    }
})


app.post("/api/jobs", isAuthenticated, async (req, res) => {
  const validation = createJobSchema.safeParse(req.body);

  if (!validation.success) {
    res.status(400).json({
      success: false,
      errors: validation.error.issues,
    });
    return;
  }

  try {
    if (!req.user) {
      res.status(401).json({
        success: false,
        message: "Authentication required",
      });
      return;
    }

    const { title, payload } = validation.data;

    const job = await prisma.job.create({
      data: {
        userId: req.user.id,
        title,
        ...(payload !== undefined ? { payload: payload as object } : {}),
      },
    });

    res.status(201).json({
      success: true,
      data: job,
    });
  } catch (error) {
    console.error("Failed to create job:", error);

    res.status(500).json({
      success: false,
      message: "Failed to create job",
    });
  }
});



app.listen(PORT, () => {
    console.log(`TaskForge API running on port ${PORT}`);
})