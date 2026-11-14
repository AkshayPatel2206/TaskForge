
import { Router } from "express";
import prisma from "../lib/prisma.js";
import { Redis } from "ioredis";

const router = Router();

const redis = new Redis({
    host: process.env.REDIS_HOST || "localhost",
    port: Number(process.env.REDIS_PORT || 6379),
    maxRetriesPerRequest: 1,
    lazyConnect: true,
});

// Liveness check
router.get("/", (_req, res) => {
    res.status(200).json({
        success: true,
        status: "ok",
        service: "TaskForge API",
        timestamp: new Date().toISOString(),
    });
});

// Readiness check
router.get("/ready", async (_req, res) => {
    let databaseStatus = "disconnected";
    let redisStatus = "disconnected";
    let workerStatus = "stopped";
    let outboxPublisherStatus = "stopped";

    // Check PostgreSQL
    try {
        await prisma.$queryRaw`SELECT 1`;
        databaseStatus = "connected";
    } catch (error) {
        // Database check failed
    }

    // Check Redis and background process heartbeats
    try {
        const result = await redis.ping();

        if (result === "PONG") {
            redisStatus = "connected";

            const workerHeartbeat = await redis.get(
                "taskforge:health:worker"
            );

            const publisherHeartbeat = await redis.get(
                "taskforge:health:outbox-publisher"
            );

            workerStatus = workerHeartbeat ? "running" : "stopped";

            outboxPublisherStatus = publisherHeartbeat
                ? "running"
                : "stopped";
        }
    } catch (error) {
        // Redis or heartbeat check failed
    }

    // All dependencies and background processes must be healthy
    const isReady =
        databaseStatus === "connected" &&
        redisStatus === "connected" &&
        workerStatus === "running" &&
        outboxPublisherStatus === "running";

    res.status(isReady ? 200 : 503).json({
        success: isReady,
        status: isReady ? "ready" : "not ready",
        checks: {
            database: databaseStatus,
            redis: redisStatus,
            worker: workerStatus,
            outboxPublisher: outboxPublisherStatus,
        },
        timestamp: new Date().toISOString(),
    });
});

// Redis connection cleanup
process.on("SIGINT", async () => {
    await redis.quit();
});

process.on("SIGTERM", async () => {
    await redis.quit();
});

export default router;
