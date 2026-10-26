import {Queue} from "bullmq";
import "dotenv/config";

export const jobQueue = new Queue("taskforge-jobs",{
    connection:{
        host:process.env.REDIS_HOST,
        port:Number(process.env.REDIS_PORT ),
    }
})