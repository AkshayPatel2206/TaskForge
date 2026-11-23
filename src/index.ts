import express from "express";
import dotenv from "dotenv";
import { randomUUID } from "node:crypto";
import authRoutes from "./routes/auth.routes.js";
import jobRoutes from "./routes/job.routes.js";
import logger from "./lib/logger.js";
import healthRoutes from "./routes/health.routes.js";
import errorMiddleware from "./middleware/error.middleware.js";
import { apiLimiter, authLimiter } from "./middleware/rate-limit.middleware.js";
import helmet from "helmet";
dotenv.config();

const app = express();
const PORT = process.env.PORT || 4001;
app.use(helmet());

app.use(express.json());

app.use((req,res,next) => {
    const requestId = randomUUID();

    res.locals.requestId = requestId;
    res.setHeader("X-Req-Id",requestId);

    next();
})
// HTTP request logging middleware
app.use((req, res, next) => {
    const startTime = Date.now();

    res.on("finish", () => {
        const duration = Date.now() - startTime;

        logger.info(
            {
                requestId: res.locals.requestId,
                method: req.method,
                url: req.originalUrl,
                statusCode: res.statusCode,
                duration,
            },
            "HTTP request completed"
        );
    });

    next();
});
app.use("/api", apiLimiter);
app.use("/api/auth", authLimiter, authRoutes);
app.use("/api/jobs", jobRoutes);
app.use("/health", healthRoutes);

app.use(errorMiddleware);

app.listen(PORT, () => {
    logger.info(
        { port: PORT },
        "TaskForge API started successfully"
    );
});