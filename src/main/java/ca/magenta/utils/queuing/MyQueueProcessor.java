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


    @Override
    protected void stopIt() {
        super.stopIt();
        stopWait();
    }

    public MyQueueProcessor(String name, Partition partition, int queueDepth, long printEvery) {
        super(name);

        this.partition = partition;
        this.queueDepth = queueDepth;
        this.printEvery = printEvery;
        this.inputQueue = new MyBlockingQueue<>(this.queueDepth);
    }

    public void putIntoQueue(T element) throws InterruptedException {

        inputQueue.put(element);
    }

    protected T takeFromQueue() throws StopWaitAsked, InterruptedException {
        return inputQueue.take();
    }


    protected void waitWhileLocalQueueCanDrain(Runner callerRunner) throws StopWaitAsked, InterruptedException {
        inputQueue.waitForWellDrain();

    }

    public abstract void waitWhileEndDrainsCanDrain(Runner callerRunner) throws InterruptedException, StopWaitAsked;

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

    protected String buildReportString(long totalTime, float msgPerSec, long queueLength, long hiWaterMarkQueueLength, float msgPerSecSinceStart) {
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

    public void stopWait() {

        inputQueue.stopWait();
    }

}
