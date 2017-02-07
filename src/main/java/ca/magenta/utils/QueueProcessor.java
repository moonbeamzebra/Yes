package ca.magenta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class QueueProcessor extends Runner {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
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
        this.inputQueue = new ArrayBlockingQueue<Object>(this.queueDepth );
    }

    protected void putInQueueImpl(Object obj, float queueDepthWarningThreshold) throws InterruptedException {

        inputQueue.put(obj);

        if (logger.isWarnEnabled()) {
            int length = inputQueue.size();
            float percentFull = length / queueDepth;

            if (percentFull > queueDepthWarningThreshold)
                logger.warn(String.format("Queue length threashold bypassed max:[%d]; " +
                                "queue length:[%d] " +
                                "Percent:[%f] " +
                                "Threshold:[%f]",
                        queueDepth, length, percentFull, queueDepthWarningThreshold));
        }

    }


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
