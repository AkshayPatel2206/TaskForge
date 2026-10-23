import {z} from "zod";

export const registerSchema = z.object({
    email:z.email().max(254),
     password: z.string().min(8).max(72),
     name: z.string().trim().min(1).max(100).optional(),
})


export const loginSchema = z.object({
  email: z.email().max(254),
  password: z.string().min(1).max(72),
});