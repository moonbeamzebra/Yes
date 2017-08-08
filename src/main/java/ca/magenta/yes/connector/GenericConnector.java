package ca.magenta.yes.connector;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.utils.AbstractTCPServerHandler;
import ca.magenta.utils.queuing.StopWaitAsked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class GenericConnector extends AbstractTCPServerHandler {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final LogParser logParser;

    GenericConnector(AbstractTCPServer tcpServer, String partitionName, Socket handlerSocket, LogParser logParser) {

        super(tcpServer, partitionName, handlerSocket);

        this.logParser = logParser;

    }

    @Override
    public void run() {
        setDoRun(true);

        String clientIP = handlerSocket.getInetAddress().toString();
        int clientPort = handlerSocket.getPort();

        logger.info("Received a connection from {}:{}", clientIP, clientPort);

        try (
                InputStreamReader isr = new InputStreamReader(handlerSocket.getInputStream());
                BufferedReader in = new BufferedReader(isr);
                )
        {

            processQueue(in);

            in.close();
            isr.close();
            handlerSocket.close();

        } catch (IOException e) {
            if (isDoRun())
                logger.error("IOException", e);
        }

        tcpServer.removeTcpServerHandler(this);

        logger.info("Connection closed from {}:{}", clientIP, clientPort);
    }

    private void processQueue(BufferedReader in) {
        try {
            String line = ""; // Just let go in
            while ((isDoRun()) && (line != null) ) {

                logParser.waitWhileEndDrainsCanDrain(this);
                line = in.readLine();
                if (line != null) {
                    logParser.putIntoQueue(line);
                }
            }
        }catch (StopWaitAsked | IOException | InterruptedException e) {
            if (isDoRun())
                logger.error(e.getClass().getSimpleName(), e);
        }
    }

}
