package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.yes.Config;
import ca.magenta.yes.data.MasterIndex;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class ConnectorManager extends Runner {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final Config config;
    private final MasterIndex masterIndex;

    private ArrayList<TCPGenericConnector> tcpGenericConnectors;

    public ConnectorManager(Config config, MasterIndex masterIndex) {

        super("single");

        this.config = config;
        this.masterIndex = masterIndex;
    }

    @Override
    public synchronized void startInstance() throws AppException {

        tcpGenericConnectors = constructConnectors(config, masterIndex);
        for (TCPGenericConnector tcpGenericConnector : tcpGenericConnectors) {
            tcpGenericConnector.startServer();
        }

        super.startInstance();
    }

    @Override
    public synchronized void stopInstance() {

        this.stopConnectors();

        super.stopInstance();
    }


    private void stopConnectors() {
        for (TCPGenericConnector tcpGenericConnector : tcpGenericConnectors) {
            tcpGenericConnector.stopServer();
            logger.info(String.format("GenericConnector [%s] stopped", tcpGenericConnector.getName()));
        }

    }

    public void run() {

        logger.info(String.format("%s [%s] started", this.getClass().getSimpleName(), this.getName()));
        //long previousNow = System.currentTimeMillis();
        try {

            while (doRun) {
                // TODO monitor tcpGenericConnectors here
                sleep(10000);
            }
        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
        }
        logger.info(String.format("%s [%s] stopped", this.getClass().getSimpleName(), this.getName()));
    }

    private ArrayList<TCPGenericConnector> constructConnectors(Config config, MasterIndex masterIndex) throws AppException {

        ArrayList<TCPGenericConnector> tcpGenericConnectors = new ArrayList<>();
        TCPGenericConnector tcpGenericConnector;

        String[] connectors = config.getGenericConnectorPorts().split(";");

        for (String connector : connectors) {
            String[] items = connector.trim().split(",");

            if (items.length == 2) {
                try {
                    String partition = items[0].trim();
                    int port = Integer.valueOf(items[1].trim());
                    tcpGenericConnector = new TCPGenericConnector(partition, config, port, masterIndex);
                    tcpGenericConnectors.add(tcpGenericConnector);
                } catch (NumberFormatException e) {
                    throw new AppException(String.format("Bad GenericConnector port [%s]", connector), e);
                }
            } else
                throw new AppException(String.format("Bad GenericConnector [%s]", config.getGenericConnectorPorts()));
        }

        return tcpGenericConnectors;
    }


}
