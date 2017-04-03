package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.utils.QueueProcessor;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.NormalizedMsgRecord;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;


public abstract class ProcessorMgmt extends QueueProcessor {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final long cuttingTime;

    ProcessorMgmt(String name, String partition, long cuttingTime) {

        super(name, partition, Globals.getConfig().getProcessorQueueDepth(), 650000);

        this.cuttingTime = cuttingTime;
    }

    public void run() {

        try {
            Processor processor = createProcessor(inputQueue);

            logger.info(String.format("[%s] started for partition [%s]", this.getClass().getSimpleName(), partition));


            while (doRun || (!inputQueue.isEmpty())) {
                String indexPath = Globals.getConfig().getIndexBaseDirectory() + File.separator;
                String indexPathName = indexPath +
                        this.getClass().getSimpleName() + "." +
                        java.util.UUID.randomUUID();
                processor.createIndex(indexPathName);
                Thread processorThread = new Thread(processor, processor.getClass().getSimpleName());
                processorThread.start();

                if (doRun) {
                    try {
                        Thread.sleep(cuttingTime);
                    } catch (InterruptedException e) {
                        if (doRun)
                            logger.error("InterruptedException", e);
                    }
                }
                if (!doRun) {
                    // The still running processor take care of draining the queue
                    this.letDrain();
                }

                if (logger.isDebugEnabled())
                    logger.debug("Time to rotate...stop the thread");
                processor.stopIt();
                processorThread.interrupt();
                try {
                    processorThread.join();
                } catch (InterruptedException e) {
                    logger.error("InterruptedException", e);
                }
                if (logger.isDebugEnabled())
                    logger.debug("Stopped");
                if (this instanceof LongTermProcessorMgmt)
                    processor.printReport();

                try {
                    processor.commitAndClose();
                    long count = processor.getThisRunCount();
                    if (count > 0) {
                        publishIndex(processor, indexPath, indexPathName);
                    } else {
                        deleteUnusedIndex(indexPathName);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        } catch (AppException e) {
            logger.error("AppException", e);
        }

        logger.info(String.format("[%s] stopped for partition [%s]", this.getClass().getSimpleName(), partition));

    }

    abstract void publishIndex(Processor processor,
                               String indexPath,
                               String indexPathName) throws IOException, AppException;

    abstract void deleteUnusedIndex(String indexPathName);

    abstract Processor createProcessor(BlockingQueue<Object> queue) throws AppException;


    synchronized void putInQueue(NormalizedMsgRecord normalizedMsgRecord) throws InterruptedException {


        this.putInQueueImpl(normalizedMsgRecord, Globals.getConfig().getQueueDepthWarningThreshold());
    }
}
