package ca.magenta.yes.connector;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.utils.AbstractTCPServerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

public class GenericConnector extends AbstractTCPServerHandler {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final LogParser logParser;

    GenericConnector(AbstractTCPServer tcpServer, String partitionName, Socket handlerSocket, LogParser logParser) {

        super(tcpServer, partitionName, handlerSocket);

        this.logParser = logParser;

    }

    @Override
    public void run() {
        doRun = true;

        String clientIP = handlerSocket.getInetAddress().toString();
        int clientPort = handlerSocket.getPort();

        logger.info(String.format("Received a connection from %s:%s", clientIP, clientPort));

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(handlerSocket.getInputStream()));
            PrintWriter out = new PrintWriter(handlerSocket.getOutputStream());

            try {
                String line = ""; // Just let go in
                while ((doRun) && (line != null) ) {

                    if (logParser.isEndDrainsCanDrain()) {
                        line = in.readLine();
                        if (line != null) {
                            logParser.putInQueue(line);
                        }
                    }
                    else
                    {
                        logger.warn(String.format("Partition:[%s] drains baddly", getName()));

                    }
                }
            } catch (SocketException e) {
                if (doRun)
                    logger.error("SocketException", e);
            } catch (IOException e) {
                if (doRun)
                    logger.error("IOException", e);
            } catch (InterruptedException e) {
                if (doRun)
                    logger.error("InterruptedException", e);
            }

            in.close();
            out.close();
            handlerSocket.close();

        } catch (IOException e) {
            if (doRun)
                logger.error("IOException", e);
        }
        tcpServer.removeTcpServerHandler(this);

        logger.info(String.format("Connection closed from %s:%s", clientIP, clientPort));
    }

}
