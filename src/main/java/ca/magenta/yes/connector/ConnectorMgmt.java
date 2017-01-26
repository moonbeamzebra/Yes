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

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final RealTimeProcessorMgmt realTimeProcessorMgmt;

    ArrayList<TCPGenericConnector> tcpGenericConnectors;

    private volatile boolean doRun = true;

    public void stopIt() {
        doRun = false;
    }

    public ConnectorMgmt(Config config) throws IOException, AppException {

        realTimeProcessorMgmt =
                new RealTimeProcessorMgmt("RealTimeProcessorMgmt",
                        config.getRealTimeCuttingTime(),
                        config, "single");
        realTimeProcessorMgmt.startInstance();

        tcpGenericConnectors = constructConnectors(config, realTimeProcessorMgmt);
        for (TCPGenericConnector tcpGenericConnector : tcpGenericConnectors) {
            tcpGenericConnector.startServer();
        }


    }

    public void stopConnectors() {
        for (TCPGenericConnector tcpGenericConnector : tcpGenericConnectors) {
            tcpGenericConnector.stopServer();
            logger.info(String.format("GenericConnector [%s] stopped", tcpGenericConnector.getName()));
        }

        TCPServer.stopLaunchedTCPServers();

    }

    public void run() {

        logger.info("New ConnectorMgmt running");
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

    private ArrayList<TCPGenericConnector> constructConnectors(Config config,
                                                               RealTimeProcessorMgmt realTimeProcessorMgmt) throws AppException {

        ArrayList<TCPGenericConnector> tcpGenericConnectors = new ArrayList<TCPGenericConnector>();
        TCPGenericConnector tcpGenericConnector;

        String[] connectors = config.getGenericConnectorPorts().split(";");

        for (String connector : connectors) {
            String[] items = connector.trim().split(",");

            if (items.length == 2) {
                try {
                    String partition = items[0].trim();
                    int port = Integer.valueOf(items[1].trim());
                    tcpGenericConnector = new TCPGenericConnector(partition, config, port, realTimeProcessorMgmt);
                    tcpGenericConnectors.add(tcpGenericConnector);
                } catch (NumberFormatException e) {
                    throw new AppException(String.format("Bad GenericConnector port [%s]", connector), e);
                }
            } else
                throw new AppException(String.format("Bad GenericConnector [%s]", config.getGenericConnectorPorts()));
        }

        return tcpGenericConnectors;
    }


    public void stop() {

        this.stopConnectors();

        realTimeProcessorMgmt.stopAPIServer();

        realTimeProcessorMgmt.stopInstance();

    }
}
