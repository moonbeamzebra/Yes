package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.yes.data.MasterIndexRecord;
import ca.magenta.yes.data.NormalizedMsgRecord;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;


public abstract class Processor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class.getPackage().getName());

    private final String partition;

    private BlockingQueue<Object> inputQueue;

    Directory indexDir;
    IndexWriter luceneIndexWriter;

    private volatile boolean doRun = true;

    private MasterIndexRecord.RuntimeTimestamps runtimeTimestamps;
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
        runtimeTimestamps = new MasterIndexRecord.RuntimeTimestamps();
        try {

            while (doRun || !inputQueue.isEmpty()) {
                NormalizedMsgRecord normalizedMsgRecord = takeFromQueue();
                if (normalizedMsgRecord != null) {
                    long srcTimestamp = normalizedMsgRecord.getSrcTimestamp();
                    long rxTimestamp = normalizedMsgRecord.getRxTimestamp();
                    if (logger.isDebugEnabled())
                        logger.debug("Processor received: " + normalizedMsgRecord.toString());
                    runtimeTimestamps.compute(srcTimestamp, rxTimestamp);
                    try {
                        storeInLucene(normalizedMsgRecord);
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
            }

        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
            else if (logger.isDebugEnabled())
                logger.debug("Processor manager asked to stop!");
        }
        runtimeTimestamps.setRunEndTimestamp(System.currentTimeMillis());
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

    synchronized private void storeInLucene(NormalizedMsgRecord normalizedMsgRecord) throws AppException {

        normalizedMsgRecord.store(luceneIndexWriter);

        if (logger.isDebugEnabled())
            logger.debug("Document added");
    }

    public abstract void createIndex(String indexPath) throws AppException;

    MasterIndexRecord.RuntimeTimestamps getRuntimeTimestamps() {
        return runtimeTimestamps;
    }

    long getThisRunCount() {
        return thisRunCount;
    }

    synchronized Directory getIndexDir() {
        return indexDir;
    }

    private NormalizedMsgRecord takeFromQueue() throws InterruptedException {

        Object obj = inputQueue.take();
        if (obj instanceof NormalizedMsgRecord) {

            return (NormalizedMsgRecord) obj;
        } else {
            logger.error(String.format("Unexpected value type: want HashMap; got [%s]", obj.getClass().getSimpleName()));
            return null;
        }
    }
}
