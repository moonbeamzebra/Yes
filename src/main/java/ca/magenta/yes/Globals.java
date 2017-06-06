package ca.magenta.yes;

import ca.magenta.utils.AppException;
import ca.magenta.yes.api.TCPAPIServer;
import ca.magenta.yes.connector.ConnectorManager;
import ca.magenta.yes.connector.common.IndexPublisher;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.MasterIndexJdbc;
import ca.magenta.yes.data.MasterIndexLucene;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

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

    public static String getHostname() {
        return hostname;
    }

    private static final String hostname;

    private static ConnectorManager connectorManager = null;
    private static IndexPublisher indexPublisher = null;
    private static TCPAPIServer tcpAPIServer = null;
    private static MasterIndex masterIndex = null;


    static {

        String tHostname = "UNKNOWN";

        try {
            tHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error(e.getClass().getSimpleName(), e);
        }

        hostname =tHostname;

    }

    public Globals(Config config) {

        logger.info("Globals created");

        Globals.config = config;
    }

    static void startEverything() throws AppException {

        startMasterIndex(Globals.getConfig().getMasterIndexEndpoint());
        startIndexPublisher();
        startAPIServer();
        startConnectorManager();

    }

    private static void startMasterIndex(String masterIndexEndpoint) throws AppException {
        try {
            masterIndexEndpoint = masterIndexEndpoint.trim();
            if (masterIndexEndpoint.startsWith("file+lucene")) {
                masterIndexEndpoint = masterIndexEndpoint.replace("file+lucene", "file");
                URL url = new URL(masterIndexEndpoint);
                masterIndex = MasterIndexLucene.getInstance(url.getPath());
            }
            else if (masterIndexEndpoint.startsWith("jdbc")) {
                masterIndex = MasterIndexJdbc.getInstance(masterIndexEndpoint);
            }
            else
            {
                throw new AppException(String.format("Unknown protocol for master index manipulation: [%s]", masterIndexEndpoint));
            }
        }
        catch (MalformedURLException e)
        {
            throw new AppException(e);
        }
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
