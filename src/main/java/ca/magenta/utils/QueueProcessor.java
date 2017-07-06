package ca.magenta.utils;

import ca.magenta.utils.queuing.StopWaitAsked;
import ca.magenta.yes.data.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class QueueProcessor extends Runner {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final long TIME_SLEEP_TO_DRAIN = 50; // milliseconds
    private final long DRAIN_MAX_RETRY = 100; // total wait 50 X 100 = 5 seconds
    private final float DRAINING_THREASHOLD = (float) 0.70; // 70%

    protected final Partition partition;
    protected final long printEvery;
    protected final int queueDepth;
    protected final BlockingQueue<Object> inputQueue;
    protected long count = 0;



    public QueueProcessor(String name, Partition partition, int queueDepth, long printEvery) {
        super(name);

        this.partition = partition;
        this.queueDepth = queueDepth;
        this.printEvery = printEvery;
        this.inputQueue = new ArrayBlockingQueue<>(this.queueDepth);
    }

//    protected void putInQueueImpl(Object obj, float queueDepthWarningThreshold) throws InterruptedException {
//
//        inputQueue.put(obj);
//
//        if (logger.isWarnEnabled()) {
//            int length = inputQueue.size();
//            float percentFull = length / queueDepth;
//
//            if (percentFull > queueDepthWarningThreshold)
//                logger.warn(String.format("Queue length threshold bypassed max:[%d]; " +
//                                "queue length:[%d] " +
//                                "Percent:[%f] " +
//                                "Threshold:[%f]",
//                        queueDepth, length, percentFull, queueDepthWarningThreshold));
//        }
//
//    }

    protected void putInQueueImpl(Object obj, float queueDepthWarningThreshold) throws InterruptedException {

        int length = inputQueue.size();
        float percentFull = length / queueDepth;

        if (percentFull < queueDepthWarningThreshold) {
            inputQueue.put(obj);
        } else {
            logger.warn(String.format("Queue length threshold bypassed max:[%d]; " +
                            "queue length:[%d] " +
                            "Percent:[%f] " +
                            "Threshold:[%f]",
                    queueDepth, length, percentFull, queueDepthWarningThreshold));
        }
    }

    public boolean isLocalQueueCanDrain(Runner callerRunner)
    {
        int retryCount = 0;
        float percentFull = inputQueue.size() / queueDepth;
        while ( ( percentFull >= DRAINING_THREASHOLD) && (retryCount < DRAIN_MAX_RETRY) )
        {
            try {
                //logger.warn("CHOKE");
                sleep(TIME_SLEEP_TO_DRAIN);
            } catch (InterruptedException e) {
                if (callerRunner.isDoRun()) {
                    logger.error(e.getClass().getSimpleName(), e);
                }
                return false;
            }
            retryCount ++;
            percentFull = inputQueue.size() / queueDepth;
        }

        return ! (percentFull >= DRAINING_THREASHOLD);

    }

    public abstract boolean isEndDrainsCanDrain(Runner callerRunner) throws StopWaitAsked, InterruptedException;

    public synchronized void letDrain() {

        logger.info(String.format("[%s]:Test queue emptiness [%d][%s]", this.getClass().getSimpleName(), inputQueue.size(), partition.getInstanceName()));
        while (!inputQueue.isEmpty()) {
            logger.info(String.format("[%s]:Let drain [%d][%s]", this.getClass().getSimpleName(), inputQueue.size(), partition.getInstanceName()));
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                logger.error("InterruptedException", e);
            }
        }
    }

    public static String buildReportString(Partition partition,
                                           String shortName,
                                           long reportCount,
                                           long totalTime,
                                           float msgPerSec,
                                           long queueLength,
                                           long hiWaterMarkQueueLength,
                                           float msgPerSecSinceStart,
                                           int queueDepth) {

        return String.format("%s-%s: %d messages sent in %d msec; [%.0f msgs/sec] in queue: %d/%d/%d trend: [%f msgs/sec]",
                partition.getInstanceName(),
                shortName,
                reportCount,
                totalTime,
                msgPerSec,
                queueLength,
                hiWaterMarkQueueLength,
                queueDepth,
                msgPerSecSinceStart);
    }

    public String buildReportString(long totalTime, float msgPerSec, long queueLength, long hiWaterMarkQueueLength, float msgPerSecSinceStart) {
        return buildReportString(partition,
                getShortName(),
                printEvery,
                totalTime,
                msgPerSec,
                queueLength,
                hiWaterMarkQueueLength,
                msgPerSecSinceStart,
                queueDepth);
    }

    protected abstract String getShortName();
}
