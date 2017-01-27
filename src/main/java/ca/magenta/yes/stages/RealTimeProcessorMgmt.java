package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.Config;
import ca.magenta.yes.api.APIServer;
import ca.magenta.yes.api.TCPAPIServer;
import ca.magenta.yes.connector.common.IndexPublisher;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;


public class RealTimeProcessorMgmt extends ProcessorMgmt {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static IndexPublisher indexPublisher = new IndexPublisher("IndexPublisher");

    private TCPAPIServer tcpAPIServer = null;


    public RealTimeProcessorMgmt(String name, long cuttingTime, Config config, String partition) {
        super(name, partition, cuttingTime, config);

        startAPIServer(config);

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
    Processor createProcessor(BlockingQueue<HashMap<String, Object>> queue) throws AppException {

        return new RealTimeProcessor("RealTimeProcessor", partition, queue);

    }


    public static IndexPublisher indexPublisher() {
        return indexPublisher;
    }


    public void startAPIServer(Config config) {

        if ((tcpAPIServer == null) || ( ! tcpAPIServer.isAlive())) {
            // String partitionName, Config config, int port, String indexBaseDirectory
            tcpAPIServer = new TCPAPIServer(partition, config, config.getApiServerPort(),config.getIndexBaseDirectory());
            logger.info("Starting TCPAPIServer...");
            tcpAPIServer.startServer();
            logger.info("TCPAPIServer started");
        }
    }

    public void stopAPIServer() {
        try {
            tcpAPIServer.stopServer();
        } catch (Exception ex) {
        }
    }

}
