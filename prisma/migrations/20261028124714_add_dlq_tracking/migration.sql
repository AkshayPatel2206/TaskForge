-- AlterTable
ALTER TABLE "Job" ADD COLUMN     "deadLetterError" TEXT,
ADD COLUMN     "deadLetteredAt" TIMESTAMP(3);
