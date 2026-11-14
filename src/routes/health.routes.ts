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

// Liveness check: Is the API process alive?
router.get("/", (_req, res) => {
  res.status(200).json({
    success: true,
    status: "ok",
    service: "TaskForge API",
    timestamp: new Date().toISOString(),
  });
});

// Readiness check: Is PostgreSQL available?
router.get("/ready", async (_req, res) => {
  let databaseStatus = "disconnected";
  let redisStatus = "disconnected";

  try {
    await prisma.$queryRaw`SELECT 1`;
    databaseStatus = "connected";
  } catch (error) {
    // Database check failed
  }

  try {
    const result = await redis.ping();

    if (result === "PONG") {
      redisStatus = "connected";
    }
  } catch (error) {
    // Redis check failed
  }

  const isReady =
    databaseStatus === "connected" &&
    redisStatus === "connected";

  res.status(isReady ? 200 : 503).json({
    success: isReady,
    status: isReady ? "ready" : "not ready",
    checks: {
      database: databaseStatus,
      redis: redisStatus,
    },
    timestamp: new Date().toISOString(),
  });
});

process.on("SIGINT", async () => {
  await redis.quit();
});

process.on("SIGTERM", async () => {
  await redis.quit();
});

export default router;