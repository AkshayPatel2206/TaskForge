
import { Request, Response, NextFunction } from "express";
import { ZodError } from "zod";
import AppError from "../lib/app-error.js";
import logger from "../lib/logger.js";
import { Prisma } from "@prisma/client";
const errorMiddleware = (
  err: Error,
  req: Request,
  res: Response,
  _next: NextFunction
) => {
  let statusCode = 500;
  let message = "Internal Server Error";
  let errors: { field: string; message: string }[] | undefined;

  if ("type" in err && err.type === "entity.too.large") {
    statusCode = 413;
    message = "Request body is too large";
}
if (
    err instanceof SyntaxError &&
    "status" in err &&
    err.status === 400 &&
    "body" in err
) {
    statusCode = 400;
    message = "Invalid JSON request body";
}

  if (err instanceof AppError) {
    statusCode = err.statusCode;
    message = err.message;
  }

  if (err instanceof ZodError) {
    statusCode = 400;
    message = "Validation failed";

    errors = err.issues.map((issue) => ({
      field: issue.path.join("."),
      message: issue.message,
    }));
  }


if (err instanceof Prisma.PrismaClientKnownRequestError) {
  switch (err.code) {
    case "P2002":
      statusCode = 409;
      message = "A record with this value already exists";
      break;

    case "P2025":
      statusCode = 404;
      message = "Record not found";
      break;

    case "P2003":
      statusCode = 400;
      message = "Invalid related record";
      break;
  }
}


  logger.error(
    {
      requestId: res.locals.requestId,
      method: req.method,
      url: req.originalUrl,
      error: err.message,
      stack: err.stack,
    },
    "Request error"
  );

  if (process.env.NODE_ENV === "production" && statusCode === 500) {
    message = "Internal Server Error";
  }

  res.status(statusCode).json({
    success: false,
    message,
    ...(errors && { errors }),
  });
};

export default errorMiddleware;
