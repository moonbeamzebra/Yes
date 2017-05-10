package ca.magenta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class QueueProcessor extends Runner {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final long TIME_SLEEP_TO_DRAIN = 50; // milliseconds
    private final long DRAIN_MAX_RETRY = 100; // total wait 50 X 100 = 5 seconds
    private final float DRAINING_THREASHOLD = (float) 0.60; // 90%

    protected final String partition;
    protected final long printEvery;
    private final int queueDepth;
    protected final BlockingQueue<Object> inputQueue;
    protected long count = 0;



    public QueueProcessor(String name, String partition, int queueDepth, long printEvery) {
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

    public boolean isLocalQueueCanDrain()
    {
        int retryCount = 0;
        float percentFull = inputQueue.size() / queueDepth;
        while ( ( percentFull >= DRAINING_THREASHOLD) && (retryCount < DRAIN_MAX_RETRY) )
        {
            try {
                //logger.warn("CHOKE");
                sleep(TIME_SLEEP_TO_DRAIN);
            } catch (InterruptedException e) {
                if (doRun) {
                    logger.error(e.getClass().getSimpleName(), e);
                    return false;
                }
            }
            retryCount ++;
            percentFull = inputQueue.size() / queueDepth;
        }

        return ! (percentFull >= DRAINING_THREASHOLD);

    }

    public abstract boolean isEndDrainsCanDrain();

    public synchronized void letDrain() {

        logger.info(String.format("[%s]:Test queue emptiness [%d][%s]", this.getClass().getSimpleName(), inputQueue.size(), partition));
        while (!inputQueue.isEmpty()) {
            logger.info(String.format("[%s]:Let drain [%d][%s]", this.getClass().getSimpleName(), inputQueue.size(), partition));
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                logger.error("InterruptedException", e);
            }
        }
    }
}
