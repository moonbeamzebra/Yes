package ca.magenta.yes.connector;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.utils.AppException;
import ca.magenta.yes.Config;
import ca.magenta.yes.data.MasterIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class TCPGenericConnector extends AbstractTCPServer {
    public static final String SHORT_NAME = "TCPC";
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final LogParser logParser;

    private final Config config;

    TCPGenericConnector(String threadName, String partitionName, Config config, int port, MasterIndex masterIndex) {
        super(threadName, port);

        this.config = config;

        logParser = new LogParser(LogParser.SHORT_NAME + "-" + partitionName, partitionName, masterIndex);

    }

    @Override
    public void startServer() throws AppException {
        //Start drain
        logParser.startInstance();

        //Start source
        super.startServer();
    }

    @Override
    synchronized public void stopServer() {
        // Stop source
        super.stopServer();

        logParser.letDrain();

        //Stop drain
        logParser.stopInstance();

    }


    @Override
    public void run() {
        logger.info(String.format("Start listen on port [%d]", serverSocket.getLocalPort()));

        doRun = true;
        while (doRun) {
            try {

                Socket socket = serverSocket.accept();

                String nameStr = this.getName() + "-" + getClientCount();
                GenericConnector genericConnector = new GenericConnector(this, nameStr, socket, logParser);
                addTcpServerHandler(genericConnector);
                genericConnector.start();
            } catch (SocketException e) {
                if (doRun) {
                    logger.error("SocketException", e);
                }
            } catch (IOException e) {
                logger.error("IOException", e);
            }
        }
        logger.info(String.format("Stop listen on port [%d]", serverSocket.getLocalPort()));
    }
}