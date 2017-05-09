package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.Globals;
import ca.magenta.yes.connector.common.IndexPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;


public class RealTimeProcessorMgmt extends ProcessorMgmt {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static IndexPublisher indexPublisher = Globals.getIndexPublisher();


    public RealTimeProcessorMgmt(String name, long cuttingTime, String partition) {
        super(name, partition, cuttingTime);


    }

    @Override
    public synchronized void startInstance() throws AppException {
        super.startInstance();
    }

    public synchronized void stopInstance() {
        super.stopInstance();
    }


    @Override
    synchronized void publishIndex(Processor RealTimeProcessor,
                              String indexPath,
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
    Processor createProcessor(BlockingQueue<Object> queue) throws AppException {

        return new RealTimeProcessor(partition, queue);

    }


    public static IndexPublisher indexPublisher() {
        return indexPublisher;
    }


    @Override
    public boolean isEndDrainsCanDrain() {
        return isLocalQueueCanDrain();
    }
}
