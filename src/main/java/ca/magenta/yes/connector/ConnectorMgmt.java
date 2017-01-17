package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.utils.TCPServer;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;

@Component
public class ConnectorMgmt implements Runnable {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());

    //private final GenericConnector genericConnectorA;
    //private final GenericConnector genericConnectorB;
    private final RealTimeProcessorMgmt realTimeProcessorMgmt;

    ArrayList<GenericConnector> genericConnectors;

    private volatile boolean doRun = true;

    public void stopIt() {
        doRun = false;
    }

    public ConnectorMgmt(Config config) throws IOException, AppException {

        realTimeProcessorMgmt =
                new RealTimeProcessorMgmt("RealTimeProcessorMgmt",
                        config.getRealTimeCuttingTime(),
                        config, "single");
        Thread realTimeThread = new Thread(realTimeProcessorMgmt, "RealTimeProcessorMgmt");
        realTimeThread.start();

        genericConnectors = constructConnectors(config, realTimeProcessorMgmt);
        for (GenericConnector genericConnector : genericConnectors)
        {
            genericConnector.startServer();
        }


    }

    public void stopConnectors()
    {
        for (GenericConnector genericConnector : genericConnectors)
        {
            genericConnector.stopServer();
            logger.info(String.format("GenericConnector [%s] stopped", genericConnector.getName()));
        }

        TCPServer.stopLaunchedTCPServers();

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

    private ArrayList<GenericConnector> constructConnectors(Config config,
                                                            RealTimeProcessorMgmt realTimeProcessorMgmt) throws AppException {

        ArrayList<GenericConnector> genericConnectors = new ArrayList<GenericConnector>();
        GenericConnector genericConnector;

        String[] connectors = config.getGenericConnectorPorts().split(";");

        for (String connector : connectors)
        {
            String[] items = connector.trim().split(",");

            if (items.length == 2)
            {
                try {
                    String partition = items[0].trim();
                    int port = Integer.valueOf(items[1].trim());
                    genericConnector = new GenericConnector(config, realTimeProcessorMgmt, port, partition);
                    genericConnectors.add(genericConnector);
                }
                catch (NumberFormatException  e)
                {
                    throw new AppException(String.format("Bad GenericConnector port [%s]", connector),e);
                }
            }
            else
                throw new AppException(String.format("Bad GenericConnector [%s]", config.getGenericConnectorPorts()));
        }

        return genericConnectors;
    }


}
