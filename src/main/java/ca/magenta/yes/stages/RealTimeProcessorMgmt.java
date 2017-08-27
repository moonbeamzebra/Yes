package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.utils.queuing.MyBlockingQueue;
import ca.magenta.utils.queuing.StopWaitAsked;
import ca.magenta.yes.Globals;
import ca.magenta.yes.connector.common.IndexPublisher;
import ca.magenta.yes.data.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;


public class RealTimeProcessorMgmt extends ProcessorMgmt {

    static final String SHORT_NAME = "RTPM";
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static IndexPublisher indexPublisher = Globals.getIndexPublisher();


    RealTimeProcessorMgmt(String name, long cuttingTime, Partition partition) {
        super(name, partition,
                (new StringBuilder()).append(RealTimeProcessor.SHORT_NAME).append('-').append(partition.getInstanceName()).toString(), cuttingTime);


    }

    @Override
    protected long giveRandom() {
        return ThreadLocalRandom.current().nextInt(0, 10);
    }

    @Override
    synchronized void publishIndex(Processor realTimeProcessor,
                                   String today,
                                   String indexPathName) {

        try {
            realTimeProcessor.commitAndClose();

            indexPublisher.publish(realTimeProcessor.getIndexDir());

            if (logger.isTraceEnabled())
            {
                logger.debug("indexPublisher.publish");
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (IOException e) {
            logger.error("IOException", e);
        }
    }


    @Override
    synchronized void deleteUnusedIndex(String indexPathName) {
        // In case of RealTime, indexes are in RAM and will destroyed by the cabbage collector
    }

    @Override
    Processor createProcessor(MyBlockingQueue queue, int queueDepth) throws AppException {

        return new RealTimeProcessor(this, partition, queue, queueDepth);

    }


    public static IndexPublisher indexPublisher() {
        return indexPublisher;
    }


    @Override
    public void waitWhileEndDrainsCanDrain(Runner callerRunner) throws InterruptedException, StopWaitAsked {
        waitWhileLocalQueueCanDrain();
    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }
}
