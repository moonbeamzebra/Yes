package ca.magenta.yes;

import ca.magenta.utils.AppException;
import ca.magenta.yes.api.TCPAPIServer;
import ca.magenta.yes.connector.ConnectorMgmt;
import ca.magenta.yes.connector.common.IndexPublisher;
import org.springframework.stereotype.Component;

@Component
public class Globals {

    private static Config config = null;

    private static ConnectorMgmt connectorMgmt = null;
    //private static RealTimeProcessorMgmt realTimeProcessorMgmt = null;
    private static IndexPublisher indexPublisher = null;
    private static TCPAPIServer tcpAPIServer = null;


    public Globals(Config config) {

        Globals.config = config;
    }

    static void startEverything() throws AppException {

        startIndexPublisher();
        startAPIServer();
        startConnectorMgmt();

    }



    public static void stopEverything() {

        stopConnectorMgmt();
        stopAPIServer();
        stopIndexPublisher();

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
        if ((tcpAPIServer == null) || ( ! tcpAPIServer.isAlive())) {
            // String partitionName, Config config, int port, String indexBaseDirectory
            tcpAPIServer = new TCPAPIServer("single", config, config.getApiServerPort(),config.getIndexBaseDirectory());
            tcpAPIServer.startServer();
        }
    }
    private static void stopAPIServer() {
        tcpAPIServer.stopServer();
    }

    private static void startConnectorMgmt() throws AppException {

        connectorMgmt =  new ConnectorMgmt(config);

        connectorMgmt.startInstance();

    }

    private  static void stopConnectorMgmt() {

        connectorMgmt.stopInstance();
    }

    public static IndexPublisher getIndexPublisher() {
        return indexPublisher;
    }
}
