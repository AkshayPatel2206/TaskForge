import type { Request, Response, NextFunction } from "express";
import jwt, { type JwtPayload } from "jsonwebtoken"
import prisma from "../lib/prisma.js";

declare global {
    namespace Express {
        interface Request {
            user?: {
                id: string;
                email: string;
                name: string | null;
            };
        }
    }
}
export async function isAuthenticated(
    req: Request,
    res: Response,
    next: NextFunction,
) {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith("Bearer")) {
        res.status(401).json({
            success: false,
            message: "Authentication required",
        });
        return;
    }

    const token = authHeader.split(" ")[1];

    try {
        const decoded = jwt.verify(
            token,
            process.env.JWT_SECRET!
        ) as JwtPayload;

        if (typeof decoded.sub !== "string") {
            res.status(401).json({
                success: false,
                message: "Invalid token",
            });
            return;
        }

        const user = await prisma.user.findUnique({
            where: { id: decoded.sub },
            select: {
                id: true,
                email: true,
                name: true,
            }
        })

        if (!user) {
            res.status(401).json({
                success: false,
                message: "User no longer exists",
            });
            return;
        }

        req.user = user;
        next();


    } catch (error) {
        if (
      error instanceof jwt.JsonWebTokenError ||
      error instanceof jwt.TokenExpiredError
    ) {
      res.status(401).json({
        success: false,
        message: "Invalid or expired token",
      });
      return;
    }

    console.error("Authentication failed:", error);
    res.status(500).json({
      success: false,
      message: "Authentication failed",
    });
    }
}
