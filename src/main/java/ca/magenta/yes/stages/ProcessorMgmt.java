package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.utils.QueueProcessor;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Partition;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;


public abstract class ProcessorMgmt extends QueueProcessor {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    //private static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyy/MM/dd'GMT'");
    //private static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyy/MM/ddz");
//    private static final SimpleDateFormat DAY_FORMAT =
//            new SimpleDateFormat(String.format("yyyy%sMM%sddz",File.separator, File.separator));
    private static final SimpleDateFormat DAY_FORMAT =
            new SimpleDateFormat(String.format("yyyy%sMM%sdd'GMT'",File.separator, File.separator));


    private final String processorThreadName;

    private final long cuttingTime;

    ProcessorMgmt(String name, Partition partition, String processorThreadName, long cuttingTime) {

        super(name, partition, Globals.getConfig().getProcessorQueueDepth(), 650000);

        this.cuttingTime = cuttingTime;

        this.processorThreadName = processorThreadName;

    }

    public void run() {

        try {
            DAY_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));

            Processor processor = createProcessor(inputQueue, queueDepth);

            logger.info(String.format("[%s] started for partition [%s]", this.getClass().getSimpleName(), partition));


            while (doRun || (!inputQueue.isEmpty())) {
                String relativePathName = forgeRelativePathName();
                //String relativePathName = partition + File.separator + DAY_FORMAT.format(System.currentTimeMillis());
                String basePath = Globals.getConfig().getIndexBaseDirectory();
                String tempIndexPathName = NormalizedMsgRecord.forgeTempIndexName(basePath, relativePathName, this.getClass().getSimpleName());
                processor.createIndex(tempIndexPathName);
                Thread processorThread = new Thread(processor, this.processorThreadName);
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
                        publishIndex(processor, basePath, relativePathName, tempIndexPathName);
                    } else {
                        deleteUnusedIndex(tempIndexPathName);
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

    protected abstract long giveRandom();

    protected String forgeRelativePathName()
    {
        return partition.getName() + File.separator +
                Globals.getHostname() + File.separator +
                DAY_FORMAT.format(System.currentTimeMillis());
    };

    abstract void publishIndex(Processor processor,
                               String indexPath,
                               String today,
                               String indexPathName) throws IOException, AppException;

    abstract void deleteUnusedIndex(String indexPathName);

    abstract Processor createProcessor(BlockingQueue<Object> queue, int queueDepth) throws AppException;


    synchronized void putInQueue(NormalizedMsgRecord normalizedMsgRecord) throws InterruptedException {


        this.putInQueueImpl(normalizedMsgRecord, Globals.getConfig().getQueueDepthWarningThreshold());
    }
}
