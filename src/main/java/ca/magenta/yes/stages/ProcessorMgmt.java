package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.utils.queuing.MyBlockingQueue;
import ca.magenta.utils.queuing.MyQueueProcessor;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Partition;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;


public abstract class ProcessorMgmt extends MyQueueProcessor<NormalizedMsgRecord> {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final SimpleDateFormat dayFormat =
            new SimpleDateFormat(String.format("yyyy%sMM%sdd'GMT'", File.separator, File.separator));


    private final String processorThreadName;

    private final long cuttingTime;

    private long soFarHiWaterMarkQueueLength = 0;

    ProcessorMgmt(String name, Partition partition, String processorThreadName, long cuttingTime) {

        super(name, partition, Globals.getConfig().getProcessorQueueDepth(), 650000);

        this.cuttingTime = cuttingTime;

        this.processorThreadName = processorThreadName;

    }

    @Override
    public void run() {

        try {
            dayFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            logger.info("[{}] started for partition [{}]", this.getClass().getSimpleName(), partition.getInstanceName());

            Processor processor;

            while (doRun || (!inputQueue.isEmpty())) {
                processor = createProcessor(inputQueue, queueDepth);
                String relativePathName = forgeRelativePathName();
                String baseTmpPath = Globals.getConfig().getTmpIndexBaseDirectory();
                String tempIndexPathName = NormalizedMsgRecord.forgeTempIndexName(baseTmpPath, relativePathName, this.getClass().getSimpleName());
                processor.createIndex(tempIndexPathName);
                Thread processorThread = new Thread(processor, this.processorThreadName);
                inputQueue.resetWaitState();
                processorThread.start();

                if (doRun) {
                    try {
                        Thread.sleep(cuttingTime + this.giveRandom());
                    } catch (InterruptedException e) {
                        if (doRun)
                            logger.error("InterruptedException", e);
                    }
                }
                if (!doRun) {
                    // The still running processor take care of draining the queue
                    this.letDrain();
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Time to rotate...stop the thread");
                }
                processor.stopIt();
                //inputQueue.stopWait();
                //processorThread.interrupt();
                try {
                    processorThread.join();
                } catch (InterruptedException e) {
                    logger.error("InterruptedException", e);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Stopped");
                }
                if (this instanceof LongTermProcessorMgmt) {
                    soFarHiWaterMarkQueueLength = processor.printReport(soFarHiWaterMarkQueueLength);
                }

                try {
                    long count = processor.getThisRunCount();
                    if (count > 0) {
                        publishIndex(processor, relativePathName, tempIndexPathName);
                    } else {
                        processor.commitAndClose();
                        deleteUnusedIndex(tempIndexPathName);
                    }
                } catch (IOException e) {
                    logger.error(e.getClass().getSimpleName(), e);
                } catch (Throwable e) {
                    logger.error(e.getClass().getSimpleName(), e);
                }

            }
        } catch (AppException e) {
            logger.error("AppException", e);
        }

        logger.info("[%s] stopped for partition [{}]", this.getClass().getSimpleName(), partition.getInstanceName());

    }

    protected abstract long giveRandom();

    private String forgeRelativePathName() {
        return partition.getName() + File.separator +
                Globals.getHostname() + File.separator +
                dayFormat.format(System.currentTimeMillis());
    }

    abstract void publishIndex(Processor processor,
                               String today,
                               String indexPathName) throws IOException, AppException;

    abstract void deleteUnusedIndex(String indexPathName);

    abstract Processor createProcessor(MyBlockingQueue queue, int queueDepth) throws AppException;


    synchronized void putInQueue(NormalizedMsgRecord normalizedMsgRecord) throws InterruptedException {


        this.putIntoQueue(normalizedMsgRecord);
    }

    long getSoFarHiWaterMarkQueueLength() {
        return soFarHiWaterMarkQueueLength;
    }

    void setSoFarHiWaterMarkQueueLength(long soFarHiWaterMarkQueueLength) {
        this.soFarHiWaterMarkQueueLength = soFarHiWaterMarkQueueLength;
    }

}
