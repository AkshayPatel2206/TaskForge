import { z } from "zod";

export const createJobSchema = z.object({
  title: z.string().trim().min(1, "title is required").max(200),
  payload: z.unknown().optional(),
});