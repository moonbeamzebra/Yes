package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.data.NormalizedMsgRecord;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;


public abstract class Processor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class.getPackage().getName());


    private final String partition;


    private BlockingQueue<Object> inputQueue;


    Directory indexDir;
    IndexWriter luceneIndexWriter;

    private volatile boolean doRun = true;

    private RunTimeStamps runTimeStamps;
    private final long startTime;
    private long hiWaterMarkQueueLength = 0;
    private long previousNow = System.currentTimeMillis();

    private long count = 0;
    private long thisRunCount = 0;
    private long reportCount = 0;


    synchronized void stopIt() {
        doRun = false;
    }

    private static final long printEvery = 100000;


    Processor(String partition, BlockingQueue<Object> inputQueue) throws AppException {

        this.partition = partition;
        this.inputQueue = inputQueue;

        this.startTime = System.currentTimeMillis();

    }

    public void run() {
        if (logger.isDebugEnabled())
            logger.debug(String.format("New [%s] running", this.getClass().getSimpleName()));
        doRun = true;
        thisRunCount = 0;
        reportCount = 0;
        runTimeStamps = new RunTimeStamps();
        try {

            while (doRun || !inputQueue.isEmpty()) {
                HashMap<String, Object> message = takeFromQueue();
                long srcTimestamp = (long) message.get(NormalizedMsgRecord.SOURCE_TIMESTAMP_FIELD_NAME);
                long rxTimestamp = (long) message.get(NormalizedMsgRecord.RECEIVE_TIMESTAMP_FIELD_NAME);
                runTimeStamps.compute(srcTimestamp, rxTimestamp);
                if (logger.isDebugEnabled())
                    logger.debug("Processor received: " + message);
                try {
                    storeInLucene(message);
                    count++;
                    thisRunCount++;
                    reportCount++;

                } catch (AppException e) {
                    logger.error("AppException", e);
                }

                if (reportCount == printEvery) {
                    printReport();
                }
            }

        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
            else if (logger.isDebugEnabled())
                logger.debug("Processor manager asked to stop!");
        }
        runTimeStamps.setRunEndTimestamp(System.currentTimeMillis());
    }

    synchronized void commitAndClose() throws IOException {
        luceneIndexWriter.commit();
        luceneIndexWriter.close();

    }

    synchronized void printReport() {
        long queueLength = inputQueue.size();
        if (queueLength > hiWaterMarkQueueLength)
            hiWaterMarkQueueLength = queueLength;

        long now = System.currentTimeMillis();

        long totalTimeSinceStart = now - startTime;
        float msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;

        long totalTime = now - previousNow;
        float msgPerSec = ((float) reportCount / (float) totalTime) * 1000;

        System.out.println(partition + "-" + this.getClass().getSimpleName() + ": " + reportCount +
                " messages sent in " + totalTime +
                " msec; [" + msgPerSec + " msgs/sec] in queue: " + queueLength + "/" + hiWaterMarkQueueLength +
                " trend: [" + msgPerSecSinceStart + " msgs/sec] ");
        previousNow = now;
        reportCount = 0;

    }

    synchronized private void storeInLucene(HashMap<String, Object> message) throws AppException {

        NormalizedMsgRecord.store(message, luceneIndexWriter);

        if (logger.isDebugEnabled())
            logger.debug("Document added");
    }

    public abstract void createIndex(String indexPath) throws AppException;

    RunTimeStamps getRunTimeStamps() {
        return runTimeStamps;
    }

    long getThisRunCount() {
        return thisRunCount;
    }

    synchronized Directory getIndexDir() {
        return indexDir;
    }

    static class RunTimeStamps {

        private long olderSrcTimestamp;
        private long newerSrcTimestamp;

        private long olderRxTimestamp;
        private long newerRxTimestamp;

        private final long runStartTimestamp;
        private long runEndTimestamp;

        RunTimeStamps() {
            runStartTimestamp = System.currentTimeMillis();

            olderSrcTimestamp = Long.MAX_VALUE;
            newerSrcTimestamp = 0;

            olderRxTimestamp = Long.MAX_VALUE;
            newerRxTimestamp = 0;
        }

        void compute(long srcTimestamp, long rxTimestamp) {

            if (srcTimestamp < olderSrcTimestamp)
                olderSrcTimestamp = srcTimestamp;
            if (srcTimestamp > newerSrcTimestamp)
                newerSrcTimestamp = srcTimestamp;

            if (rxTimestamp < olderRxTimestamp)
                olderRxTimestamp = rxTimestamp;
            if (rxTimestamp > newerRxTimestamp)
                newerRxTimestamp = rxTimestamp;
        }

        long getOlderSrcTimestamp() {
            return olderSrcTimestamp;
        }

        long getNewerSrcTimestamp() {
            return newerSrcTimestamp;
        }

        long getOlderRxTimestamp() {
            return olderRxTimestamp;
        }

        long getNewerRxTimestamp() {
            return newerRxTimestamp;
        }

        long getRunStartTimestamp() {
            return runStartTimestamp;
        }

        long getRunEndTimestamp() {
            return runEndTimestamp;
        }

        void setRunEndTimestamp(long runEndTimestamp) {
            this.runEndTimestamp = runEndTimestamp;
        }
    }

    private HashMap<String, Object> takeFromQueue() throws InterruptedException {

        Object obj = inputQueue.take();
        if (obj instanceof HashMap) {

            return (HashMap<String, Object>) obj;
        } else {
            logger.error(String.format("Unexpected value type: want HashMap; got [%s]", obj.getClass().getSimpleName()));
            return null;
        }
    }


}
