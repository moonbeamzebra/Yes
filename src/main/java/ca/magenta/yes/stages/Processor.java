package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.utils.QueueProcessor;
import ca.magenta.utils.queuing.MyBlockingQueue;
import ca.magenta.utils.queuing.StopWaitAsked;
import ca.magenta.yes.data.MasterIndexRecord;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Partition;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;


public abstract class Processor implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final ProcessorMgmt processorMgmt;

    private final Partition partition;

    private MyBlockingQueue<Object> inputQueue;

    Directory indexDir;
    IndexWriter luceneIndexWriter;

    private volatile boolean doRun = true;

    private MasterIndexRecord.RuntimeTimestamps runtimeTimestamps;
    private final int queueDepth;
    private final long startTime;
    private long previousNow = System.currentTimeMillis();

    private long count = 0;
    private long thisRunCount = 0;
    private long reportCount = 0;

    synchronized void stopIt() {
        doRun = false;

        inputQueue.stopWait();
    }

    private static final long PRINT_EVERY = 50000;

    Processor(ProcessorMgmt processorMgmt, Partition partition, MyBlockingQueue<Object> inputQueue, int queueDepth) {

        this.processorMgmt = processorMgmt;

        this.partition = partition;
        this.inputQueue = inputQueue;
        this.queueDepth = queueDepth;

        this.startTime = System.currentTimeMillis();

    }

    @Override
    public void run() {
        if (logger.isDebugEnabled()) {
            logger.debug("New [{}] running", this.getClass().getSimpleName());
        }
        doRun = true;
        thisRunCount = 0;
        reportCount = 0;
        runtimeTimestamps = new MasterIndexRecord.RuntimeTimestamps();
        try {

            while (doRun || !inputQueue.isEmpty()) {
                try {
                    NormalizedMsgRecord normalizedMsgRecord = takeFromQueue();
                    if (normalizedMsgRecord != null) {
                        long srcTimestamp = normalizedMsgRecord.getSrcTimestamp();
                        long rxTimestamp = normalizedMsgRecord.getRxTimestamp();
                        if (logger.isTraceEnabled()) {
                            logger.trace("Processor received: {}", normalizedMsgRecord.toString());
                        }
                        runtimeTimestamps.compute(srcTimestamp, rxTimestamp);
                        try {
                            storeInLucene(normalizedMsgRecord);
                            count++;
                            thisRunCount++;
                            reportCount++;

                        } catch (AppException e) {
                            logger.error("AppException", e);
                        } catch (Throwable e) {
                            logger.error(e.getClass().getSimpleName(), e);
                        }

                        if (reportCount == PRINT_EVERY) {

                            long soFarHiWaterMarkQueueLength = printReport(processorMgmt.getSoFarHiWaterMarkQueueLength());
                            processorMgmt.setSoFarHiWaterMarkQueueLength(soFarHiWaterMarkQueueLength);
                        }
                    }
                }
                catch (StopWaitAsked e)
                {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Stop Wait Asked !");
                    }
                }
            }

        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
            else if (logger.isDebugEnabled()) {
                logger.debug("Processor manager asked to stop!");
            }
        }


        runtimeTimestamps.setRunEndTimestamp(System.currentTimeMillis());
    }

    synchronized void commitAndClose() throws IOException {
        luceneIndexWriter.commit();
        luceneIndexWriter.close();

    }

    synchronized long printReport(long previousHiWaterMarkQueueLength) {
        long queueLength = inputQueue.size();

        long newHiWaterMarkQueueLength = previousHiWaterMarkQueueLength;
        if (queueLength > newHiWaterMarkQueueLength) {
            newHiWaterMarkQueueLength = queueLength;
        }

        long now = System.currentTimeMillis();

        long totalTimeSinceStart = now - startTime;
        float msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;

        long totalTime = now - previousNow;
        float msgPerSec = ((float) reportCount / (float) totalTime) * 1000;

        //if (reportCount > 0) {
            String report = QueueProcessor.buildReportString(partition,
                    this.getShortName(),
                    reportCount,
                    totalTime,
                    msgPerSec,
                    queueLength,
                    newHiWaterMarkQueueLength,
                    msgPerSecSinceStart,
                    queueDepth);

            System.out.println(report);
        //}
        previousNow = now;
        reportCount = 0;

        return newHiWaterMarkQueueLength;

    }

    protected abstract String getShortName();

    private synchronized void storeInLucene(NormalizedMsgRecord normalizedMsgRecord) throws AppException {

        normalizedMsgRecord.store(luceneIndexWriter);

        if (logger.isTraceEnabled()) {
            logger.trace("Document added");
        }
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

    private NormalizedMsgRecord takeFromQueue() throws InterruptedException, StopWaitAsked {

        Object obj = inputQueue.take();
        if (obj instanceof NormalizedMsgRecord) {

            return (NormalizedMsgRecord) obj;
        } else {
            logger.error("Unexpected value type: want HashMap; got [{}]", obj.getClass().getSimpleName());
            return null;
        }
    }

}
