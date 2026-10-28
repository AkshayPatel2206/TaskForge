import {Queue} from "bullmq";
import "dotenv/config";

const connection ={
        host:process.env.REDIS_HOST,
        port:Number(process.env.REDIS_PORT ),
};
export const jobQueue = new Queue("taskforge-jobs",{
    connection
})
export const deadLetterQueue = new Queue("askforge-dead-letter",{
    connection,
})