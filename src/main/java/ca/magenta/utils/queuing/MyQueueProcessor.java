package ca.magenta.utils.queuing;

import ca.magenta.utils.Runner;
import ca.magenta.yes.data.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MyQueueProcessor<T> extends Runner {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected final Partition partition;
    protected final long printEvery;
    protected final int queueDepth;
    protected final MyBlockingQueue<T> inputQueue;
    protected long count = 0;



    public MyQueueProcessor(String name, Partition partition, int queueDepth, long printEvery) {
        super(name);

        this.partition = partition;
        this.queueDepth = queueDepth;
        this.printEvery = printEvery;
        this.inputQueue = new MyBlockingQueue<>(this.queueDepth);
    }

    protected void putIntoQueue(T element, float queueDepthWarningThreshold) throws InterruptedException {

        inputQueue.put(element);
    }

    public boolean isLocalQueueCanDrain(Runner callerRunner) throws StopWaitAsked, InterruptedException {
        inputQueue.waitForWellDrain();

        return true;

    }

    public abstract boolean isEndDrainsCanDrain(Runner callerRunner) throws InterruptedException, StopWaitAsked;

//    public synchronized void letDrain() {
//
//        logger.info(String.format("[%s]:Test queue emptiness [%d][%s]", this.getClass().getSimpleName(), inputQueue.size(), partition.getInstanceName()));
//        while (!inputQueue.isEmpty()) {
//            logger.info(String.format("[%s]:Let drain [%d][%s]", this.getClass().getSimpleName(), inputQueue.size(), partition.getInstanceName()));
//            try {
//                wait(200);
//            } catch (InterruptedException e) {
//                logger.error("InterruptedException", e);
//            }
//        }
//    }

    public void letDrain() {

        inputQueue.letDrain(String.format("[%s][%s]", this.getClass().getSimpleName(), partition.getInstanceName()));

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
