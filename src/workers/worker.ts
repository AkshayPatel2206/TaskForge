import "dotenv/config";
import { Worker } from "bullmq";
import  prisma  from "../lib/prisma.js";



const worker = new Worker("taskforge-jobs",async(bullJob) => {
    const {jobId} =  bullJob.data as {jobId:string};

    console.log(`Processing job: ${jobId}`);

     const job = await prisma.job.findUnique({
      where: { id: jobId },
    });

     if (!job) {
      throw new Error(`Job ${jobId} not found in PostgreSQL`);
    }

    if (job.status !== "PENDING") {
      console.log(`Skipping job ${jobId} with status ${job.status}`);
      return;
    }

    const updated = await prisma.job.updateMany({
        where:{
            id:jobId,
            status:"PENDING",
        },
        data:{
            status:"RUNNING"
        }
    })

     if (updated.count === 0) {
      console.log(`Job ${jobId} was already claimed or changed`);
      return;
    }

    try {
        await new Promise((resolve) => setTimeout(resolve,3000));

        await prisma.job.update({
            where:{id:jobId},
            data:{
                status:"COMPLETED",
                result:{message:"Job processed successfully"},
            }
            
        })
        console.log(`Job ${jobId} completed successfully`);
    } catch (error) {
        await prisma.job.update({
        where: { id: jobId },
        data: { status: "FAILED" },
      });
       throw error;
    }
},
{
    connection:{
        host: process.env.REDIS_HOST || "localhost",
      port: Number(process.env.REDIS_PORT || 6379),
    }
})

worker.on("completed",(job)=>{
    console.log(`Queue job ${job.id} compleated`);
});

worker.on("failed", (job, error) => {
  console.error(`Queue job ${job?.id} failed:`, error.message);
});

console.log("TaskForge worker is running");