package ca.magenta.yes;

import ca.magenta.utils.AppException;
import ca.magenta.yes.api.TCPAPIServer;
import ca.magenta.yes.connector.ConnectorManager;
import ca.magenta.yes.connector.common.IndexPublisher;
import ca.magenta.yes.data.MasterIndex;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Globals {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Application.class.getPackage().getName());

    public enum DrivingTimestamp {
        RECEIVE_TIME, SOURCE_TIME
    }

    public static void setConfig(Config config) {
        Globals.config = config;
    }

    private static Config config;

    private static ConnectorManager connectorManager = null;
    private static IndexPublisher indexPublisher = null;
    private static TCPAPIServer tcpAPIServer = null;
    private static MasterIndex masterIndex = null;


    public Globals(Config config) {

        logger.info("Globals created");

        Globals.config = config;
    }

    static void startEverything() throws AppException {

        startMasterIndex();
        startIndexPublisher();
        startAPIServer();
        startConnectorManager();

    }

    private static void startMasterIndex() throws AppException {
        masterIndex = MasterIndex.getInstance();
    }


    static void stopEverything() {

        stopConnectorManager();
        stopAPIServer();
        stopIndexPublisher();
        stoptMasterIndex();

    }

    private static void stoptMasterIndex() {

        masterIndex.close();
    }

    private static void startIndexPublisher() {
        indexPublisher = new IndexPublisher("IndexPublisher");
        // TODO
        // indexPublisher.startInstance()
    }

    private static void stopIndexPublisher() {
        // TODO
        // indexPublisher.stopInstance()

    }

    private static void startAPIServer() throws AppException {
        if ((tcpAPIServer == null) || (!tcpAPIServer.isAlive())) {
            tcpAPIServer = new TCPAPIServer("single",
                    config.getApiServerPort(),
                    config.getIndexBaseDirectory(),
                    masterIndex);
            tcpAPIServer.startServer();
        }
    }

    private static void stopAPIServer() {
        tcpAPIServer.stopServer();
    }

    private static void startConnectorManager() throws AppException {

        connectorManager = new ConnectorManager(config, masterIndex);

        connectorManager.startInstance();

    }

    private static void stopConnectorManager() {

        connectorManager.stopInstance();
    }

    public static IndexPublisher getIndexPublisher() {
        return indexPublisher;
    }

    public static Config getConfig() {

        return config;
    }
}
