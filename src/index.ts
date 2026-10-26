import express from "express";
import dotenv from "dotenv";

import authRoutes from "./routes/auth.routes.js";
import jobRoutes from "./routes/job.routes.js";
dotenv.config();

const app = express();
const PORT = process.env.PORT || 4000;

app.use(express.json());
app.use("/api/auth", authRoutes);
app.use("/api/jobs", jobRoutes);
app.get("/health", (_req, res) => {
    res.json({
        status: "ok",
        service: "TaskForge Api"
    })
})


app.listen(PORT, () => {
    console.log(`TaskForge API running on port ${PORT}`);
})