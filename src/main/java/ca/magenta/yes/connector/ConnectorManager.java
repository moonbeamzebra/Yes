package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.yes.Config;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.Partition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class ConnectorManager extends Runner {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final Object connectorManagerMonitor = new Object();

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

        stopIt();

        synchronized (connectorManagerMonitor)
        {
            connectorManagerMonitor.notifyAll();
        }

        try {
            this.join();
        } catch (InterruptedException e) {
            logger.error(e.getClass().getSimpleName(), e);
            Thread.currentThread().interrupt();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("{} [{}] stopped", this.getClass().getSimpleName(), this.getName());
        }
    }


    private void stopConnectors() {
        for (TCPGenericConnector tcpGenericConnector : tcpGenericConnectors) {
            tcpGenericConnector.stopServer();
            logger.info("GenericConnector [{}] stopped", tcpGenericConnector.getName());
        }

    }

    @Override
    public void run() {

        logger.info("{} [{}] started", this.getClass().getSimpleName(), this.getName());
        try {

            while (isDoRun()) {
                synchronized (connectorManagerMonitor)
                {
                    // TODO monitor tcpGenericConnectors here
                    connectorManagerMonitor.wait(10000);
                }
            }
        } catch (InterruptedException e) {
            if (isDoRun())
                logger.error("InterruptedException", e);
        }
        logger.info("{} [{}] stopped", this.getClass().getSimpleName(), this.getName());
    }

    private ArrayList<TCPGenericConnector> constructConnectors(Config config, MasterIndex masterIndex) throws AppException {

        HashMap<String, HashMap<String, Integer>> partitions = new HashMap<>();
        HashSet<Integer> ports = new HashSet<>();

        ArrayList<TCPGenericConnector> tcpGenericConnectors = new ArrayList<>();
        TCPGenericConnector tcpGenericConnector;

        //String[] connectors = config.getGenericConnectorPorts().split(";");
        List<String> connectors = config.getGenericConnectorPorts();

        ObjectMapper mapper = new ObjectMapper();
        int index = 0;
        for (String connector : connectors) {
            if (connector != null) {
                Partition partition = Partition.fromJSon(mapper, connector);
                try {
                    String partitionName = partition.getName();
                    int partitionPort = partition.getPort();
                    String partitionInstance = partition.getInstance();

                    if (!partitionName.contains("-") &&
                            ((partitionInstance == null) ||
                                    (!partitionInstance.contains("-") && !partitionInstance.matches("^\\d+$")))) {

                        HashMap<String, Integer> partitionInstances = partitions.get(partitionName);
                        if (partitionInstances == null) {
                            partitionInstances = new HashMap<String, Integer>();
                        }
                        int instanceIndex = partitionInstances.size();

                        if (partitionInstance == null) {
                            // Default partitionInstance to an index
                            partitionInstance = Integer.toString(instanceIndex);
                            partition.setInstance(partitionInstance);

                        }

                        if (!partitionInstances.containsKey(partitionInstance) && !ports.contains(partitionPort)) {

                            tcpGenericConnector = new TCPGenericConnector(TCPGenericConnector.SHORT_NAME + "-" + partition.getInstanceName(),
                                    partition,
                                    config,
                                    partitionPort,
                                    masterIndex);
                            tcpGenericConnectors.add(tcpGenericConnector);

                            partitionInstances.put(partitionInstance, instanceIndex);
                            partitions.put(partitionName, partitionInstances);
                            ports.add(partitionPort);
                        } else {
                            throw new AppException(String.format("GenericConnector partitionInstance/port duplicated [%d]=[%s]", index, connector));
                        }
                    } else {
                        throw new AppException(String.format("Bad GenericConnector partition name or instance; '-' or decimal only not allowed [%d]=[%s]", index, connector));
                    }
                } catch (NumberFormatException e) {
                    throw new AppException(String.format("Bad GenericConnector port [%d]=[%s]", index, connector), e);
                }
            }
            index++;
        }

        return tcpGenericConnectors;
    }


}
