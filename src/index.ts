import express from "express";
import dotenv from "dotenv";

import authRoutes from "./routes/auth.routes.js";
import jobRoutes from "./routes/job.routes.js";
import logger from "./lib/logger.js";
dotenv.config();

const app = express();
const PORT = process.env.PORT || 4001;

app.use(express.json());

app.use((req,res,next) => {
    const startTime = Date.now();

    res.on("finish", () => {
        const duration = Date.now() - startTime;

        logger.info({
            method:req.method,
            url:req.originalUrl,
            statusCode:res.statusCode,
            duration
        }),
         "HTTP request completed"
    })
    next();
})
app.use("/api/auth", authRoutes);
app.use("/api/jobs", jobRoutes);
app.get("/health", (_req, res) => {
    res.json({
        status: "ok",
        service: "TaskForge Api"
    })
})


app.listen(PORT, () => {
    logger.info(
        { port: PORT },
        "TaskForge API started successfully"
    );
});