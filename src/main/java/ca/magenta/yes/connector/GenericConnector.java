package ca.magenta.yes.connector;

import ca.magenta.utils.TCPServer;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

import static java.io.FileDescriptor.in;


/**
 * Created by jean-paul.laberge on 12/19/2016.
 */
public class GenericConnector extends TCPServer {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());

    private final LogParser logParser;
    private final Thread logParserThread;

    private final String partition;

    public GenericConnector(Config config, RealTimeProcessorMgmt realTimeProcessorMgmt, int genericConnectorPort, String partition) {

        super(genericConnectorPort, partition);

        this.partition = partition;

        logger.info(String.format("GenericConnector started on port [%d] for partion [%s]", genericConnectorPort,partition));

        logParser = new LogParser(partition, config, realTimeProcessorMgmt, partition);
        logParserThread = new Thread(logParser, "LogParser");
        logParserThread.start();


    }

    @Override
    public synchronized void stopServer() {
        super.stopServer();

        logParser.stopIt();
        logParserThread.interrupt();
        try {
            logParserThread.join(20000);
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }

        logger.info(String.format("LogParser [%s] stopped",logParser.getName()));

    }


    public void run(Socket data) {
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(data.getInputStream()));

            InetAddress clientAddress = data.getInetAddress();
            int port = data.getPort();
            logger.info("Connected to client: " + clientAddress.getHostAddress() + ":" + port);


            String inputLine;

            while ( (! shouldStop) && (inputLine = in.readLine()) != null) {
                logger.debug(String.format("Client[%b]: [%s]", shouldStop, inputLine));
                try {
                    logParser.putInQueue(inputLine);
                } catch (InterruptedException e) {
                    if (! shouldStop)
                        logger.error("InterruptedException", e);
                }
            }

        } catch (IOException e) {
            if (! shouldStop)
                logger.error("IOException", e);
        } catch (Throwable e) {
            logger.error("Throwable", e);
        }
        if ( in != null ) {
            try {
                in.close();
                data.close();
                logger.info("IN stream closed");
            } catch (IOException e) {
                logger.error("IOException", e);
            }
        }
    }
}
