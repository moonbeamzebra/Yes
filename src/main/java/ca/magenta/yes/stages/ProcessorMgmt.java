package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.Config;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public abstract class ProcessorMgmt implements Runnable {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());


    private final String name;

    final String partition;


    private final long cuttingTime;
    final Config config;


    private final BlockingQueue<HashMap<String, Object>> inputQueue;

    private volatile boolean doRun = true;

    public void stopIt() {
        doRun = false;
    }

    public ProcessorMgmt(String name, String partition, long cuttingTime, Config config) {

        this.name = name;
        this.partition = partition;
        this.cuttingTime = cuttingTime;
        this.inputQueue = new ArrayBlockingQueue<HashMap<String, Object>>(config.getProcessorQueueDepth());
        this.config = config;

    }

    public void run() {

        ObjectMapper mapper = new ObjectMapper();

        //LongTermProcessor longTermProcessor = new LongTermProcessor("LongTermProcessor",
        //        inputQueue);
        try {
            Processor processor = createProcessor(inputQueue);

            logger.info(String.format("New [%s] running", this.getClass().getSimpleName()));
            try {

                while (doRun || !inputQueue.isEmpty()) {
                    String indexPath = config.getIndexBaseDirectory() + File.separator;
                    String indexPathName = indexPath +
                            this.getClass().getSimpleName() + "." +
                            java.util.UUID.randomUUID();
                    processor.createIndex(indexPathName);
                    Thread processorThread = new Thread(processor, processor.getClass().getSimpleName());
                    processorThread.start();

                    Thread.sleep(cuttingTime);

                    if (logger.isDebugEnabled())
                        logger.debug("Time to rotate...stop the thread");
                    processor.stopIt();
                    processorThread.interrupt();
                    processorThread.join();
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
            } catch (InterruptedException e) {
                if (doRun)
                    logger.error("InterruptedException", e);
            }
        } catch (AppException e) {
            logger.error("AppException", e);
        }

    }

//    public BlockingQueue<LogstashMessage> getInputQueue() {
//        return inputQueue;
//    }

    abstract void publishIndex(Processor processor,
                               String indexPath,
                               String indexPathName) throws IOException, AppException;

    abstract void deleteUnusedIndex(String indexPathName);

    abstract Processor createProcessor(BlockingQueue<HashMap<String, Object>> queue) throws AppException;


    public void putInQueue(HashMap<String, Object> logstashMessage) throws InterruptedException {

        inputQueue.put(logstashMessage);

        if (logger.isWarnEnabled()) {
            int length = inputQueue.size();
            float percentFull = length / config.getProcessorQueueDepth();

            if (percentFull > config.getQueueDepthWarningThreshold())
                logger.warn(String.format("Queue length threashold bypassed max:[%d]; " +
                                "queue length:[%d] " +
                                "Percent:[%f] " +
                                "Threshold:[%f]",
                        config.getProcessorQueueDepth(), length, percentFull, config.getQueueDepthWarningThreshold()));
        }

    }
}
