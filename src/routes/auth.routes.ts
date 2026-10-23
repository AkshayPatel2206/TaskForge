import { Router } from "express";

import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import { z } from "zod";
import prisma from "../lib/prisma.js";
import { loginSchema, registerSchema } from "../schemas/auth.schema.js";

const router = Router();

function generateToken(userId: string) {
    const secretKey = process.env.JWT_SECRET;

    if (!secretKey) {
        throw new Error("JWT_SECRET is not defined in environment variables");
    }

    return secretKey;
}
router.post("/register", async (req, res) => {

    const validation = registerSchema.safeParse(req.body);

    if (!validation.success) {
        res.status(400).json({
            success: false,
            message: "Invalid Registration Data",
            errors: validation.error.issues.map((issue) => ({
                field: issue.path.join("."),
                message: issue.message,
            }))

        })
        return;
    }

    try {
        const { email, password, name } = validation.data;
        const normalizedEmail = email.toLowerCase();
        const passwordhash = await bcrypt.hash(password, 10);

        const user = await prisma.user.create({
            data: {
                email: normalizedEmail,
                passwordHash: passwordhash,
                name: name || null,
            },
            select: {
                id: true,
                email: true,
                name: true,
                createdAt: true,
            },
        });
        res.status(201).json({
            success: true,
            message: "User registered successfully",
            user
        });

    } catch (error) {
        if (
            error &&
            typeof error === "object" &&
            "code" in error &&
            error.code === "P2002"
        ) {
            res.status(409).json({
                success: false,
                message: "Email is already registered",
            });
            return;
        }
        console.log(error);
        res.status(500).json({
            success: false,
            message: "An error occurred while registering the user",
        });
    }
});

router.post("/login", async (req, res) => {
    const validation = loginSchema.safeParse(req.body);
    if (!validation.success) {
        res.status(400).json({
            success: false,

            message: "Invalid Login Data",

            errors: validation.error.issues.map((issue) => ({
                field: issue.path.join("."),
                message: issue.message,

            })),

        });
        return;
    }

    try {
        const { email, password } = validation.data;
        const user = await prisma.user.findUnique({
            where: { email: email.toLowerCase() },
        });

        if (!user || !user.passwordHash || !(await bcrypt.compare(password, user.passwordHash))) {
            res.status(401).json({
                success: false,
                message: "Invalid email or password",
            });
            return;
        }

        const token = jwt.sign({ sub: user.id }, generateToken(user.id), { expiresIn: "1h" });
        res.status(200).json({
            success: true,
            message: "Login successful",
            token,
            user: {
                id: user.id,
                email: user.email,
                name: user.name,
            }
        });
    }catch (error) {
    console.error("Login error:", error);
    res.status(500).json({
        success: false,
        message: "An error occurred during login",
    });



    }
});

export default router;




