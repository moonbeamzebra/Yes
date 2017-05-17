package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.yes.Config;
import ca.magenta.yes.data.MasterIndex;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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

        HashSet<String> partitions = new HashSet<>();
        HashSet<Integer> ports = new HashSet<>();

        ArrayList<TCPGenericConnector> tcpGenericConnectors = new ArrayList<>();
        TCPGenericConnector tcpGenericConnector;

        //String[] connectors = config.getGenericConnectorPorts().split(";");
        List<String> connectors = config.getGenericConnectorPorts();

        int index = 0;
        for (String connector : connectors) {
            String[] items = connector.trim().split(",");

            if (items.length == 2) {
                try {
                    String partition = items[0].trim();
                    int port = Integer.valueOf(items[1].trim());

                    if ( ! partition.contains("-") ) {

                        if (!partitions.contains(partition) && !ports.contains(port)) {
                            tcpGenericConnector = new TCPGenericConnector(TCPGenericConnector.SHORT_NAME+"-"+partition, partition,config, port, masterIndex);
                            tcpGenericConnectors.add(tcpGenericConnector);

                            partitions.add(partition);
                            ports.add(port);
                        } else {
                            throw new AppException(String.format("GenericConnector partition/port duplicated [%d]=[%s]", index, connector));
                        }
                    }
                    else
                    {
                        throw new AppException(String.format("Bad GenericConnector partition name; '-' not allowed [%d]=[%s]", index, connector));
                    }
                } catch (NumberFormatException e) {
                    throw new AppException(String.format("Bad GenericConnector port [%d]=[%s]", connector), e);
                }
            } else {
                throw new AppException(String.format("Bad GenericConnector [%d]=[%s]", index, connector));
            }
            index++;
        }

        return tcpGenericConnectors;
    }


}
