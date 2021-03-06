package ca.magenta.yes.api;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.yes.data.MasterIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class TCPAPIServer extends AbstractTCPServer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final String indexBaseDirectory;
    private final MasterIndex masterIndex;


    public TCPAPIServer(String partitionName, int port, String indexBaseDirectory, MasterIndex masterIndex) {
        super(partitionName, port);

        this.indexBaseDirectory = indexBaseDirectory;
        this.masterIndex = masterIndex;
    }

    @Override
    public void run() {
        doRun = true;
        while (doRun) {
            try {

                Socket socket = serverSocket.accept();

                String nameStr = this.getName() + "-" + getClientCount();
                APIServer apiServer = new APIServer(this, nameStr, socket, indexBaseDirectory, masterIndex);
                addTcpServerHandler(apiServer);
                apiServer.start();
            } catch (SocketException e) {
                if (doRun) {
                    logger.error("SocketException", e);
                }
            } catch (IOException e) {
                logger.error("IOException", e);
            }
        }
    }
}