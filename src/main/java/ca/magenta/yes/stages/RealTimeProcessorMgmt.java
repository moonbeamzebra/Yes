package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.yes.Globals;
import ca.magenta.yes.connector.common.IndexPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;


public class RealTimeProcessorMgmt extends ProcessorMgmt {

    public static final String SHORT_NAME = "RTPM";
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static IndexPublisher indexPublisher = Globals.getIndexPublisher();


    public RealTimeProcessorMgmt(String name, long cuttingTime, String partition) {
        super(name, partition, (new StringBuilder()).append(RealTimeProcessor.SHORT_NAME).append('-').append(partition).toString(),cuttingTime);


    }

    @Override
    public synchronized void startInstance() throws AppException {
        super.startInstance();
    }

    public synchronized void stopInstance() {
        super.stopInstance();
    }


    @Override
    protected long giveRandom() {
        return ThreadLocalRandom.current().nextInt(0, 10);
    }

    @Override
    synchronized void publishIndex(Processor RealTimeProcessor,
                              String indexPath,
                              String today,
                              String indexPathName)
    {
        try {
            //logger.info("indexPublisher.publish");
            indexPublisher.publish(RealTimeProcessor.getIndexDir());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    synchronized  void deleteUnusedIndex(String indexPathName)
    {
        ;
    }

    @Override
    Processor createProcessor(BlockingQueue<Object> queue, int queueDepth) throws AppException {

        return new RealTimeProcessor(partition, queue, queueDepth);

    }


    public static IndexPublisher indexPublisher() {
        return indexPublisher;
    }


    @Override
    public boolean isEndDrainsCanDrain(Runner callerRunner) {
        return isLocalQueueCanDrain(callerRunner);
    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }
}
