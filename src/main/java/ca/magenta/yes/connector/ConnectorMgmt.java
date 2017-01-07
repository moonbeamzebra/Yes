package ca.magenta.yes.connector;

import ca.magenta.yes.Config;
import ca.magenta.yes.stages.Dispatcher;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Component
public class ConnectorMgmt implements Runnable {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());

    private final GenericConnector genericConnectorA;
    private final GenericConnector genericConnectorB;
    private final RealTimeProcessorMgmt realTimeProcessorMgmt;

    private volatile boolean doRun = true;

    public void stopIt() {
        doRun = false;
    }

    public ConnectorMgmt(Config config) throws IOException {

        realTimeProcessorMgmt =
                new RealTimeProcessorMgmt("RealTimeProcessorMgmt",
                        config.getRealTimeCuttingTime(),
                        config, "single");
        Thread realTimeThread = new Thread(realTimeProcessorMgmt, "RealTimeProcessorMgmt");
        realTimeThread.start();

        genericConnectorA = new GenericConnector(config,
                config.getGenericConnectorPortA(),
                Integer.toString(config.getGenericConnectorPortA()),
                realTimeProcessorMgmt);

        genericConnectorA.startServer();

        genericConnectorB = new GenericConnector(config,
                config.getGenericConnectorPortB(),
                Integer.toString(config.getGenericConnectorPortB()),
                realTimeProcessorMgmt);

        genericConnectorB.startServer();

    }

    public void run() {

        logger.info("New LogParser running");
        long previousNow = System.currentTimeMillis();
        long now;
        long totalTime;
        float msgPerSec;
        try {

            while (doRun) {
                wait();
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }
    }


}
