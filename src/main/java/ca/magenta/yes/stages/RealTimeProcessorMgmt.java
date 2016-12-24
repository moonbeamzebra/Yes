package ca.magenta.yes.stages;


import ca.magenta.yes.Config;
import ca.magenta.yes.api.APIServer;
import ca.magenta.yes.connector.common.IndexPublisher;
import ca.magenta.yes.data.LogstashMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;


public class RealTimeProcessorMgmt extends ProcessorMgmt {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());

    private static IndexPublisher indexPublisher = new IndexPublisher("IndexPublisher");

    private APIServer apiServer = null;


    public RealTimeProcessorMgmt(String name, long cuttingTime, Config config) {
        super(name, cuttingTime, config);

        startAPIServer();

    }

    @Override
    synchronized void publishIndex(Processor RealTimeProcessor,
                              String indexPath,
                              String indexPathName)
    {
        try {
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
    Processor createProcessor(BlockingQueue<LogstashMessage> queue) {

        return new RealTimeProcessor("RealTimeProcessor",
                queue);

    }


    public static IndexPublisher indexPublisher() {
        return indexPublisher;
    }


    public void startAPIServer() {

        try {

            if ((apiServer == null) || ( ! apiServer.getRunner().isAlive())) {
                apiServer = new APIServer(config.getApiServerPort(),"APISrvr");
                logger.info("Starting APIServer...");
                apiServer.startServer();
                logger.info("APIServer started");
            }
        } catch (IOException e) {

            logger.error("", e);
        }
    }

    public void stopAPIServer() {
        try {
            apiServer.stopServer();
        } catch (Exception ex) {
        }
    }

}
