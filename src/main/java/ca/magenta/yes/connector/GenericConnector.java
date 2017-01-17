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


    private BlockingQueue<String> outputQueue;

    private final LogParser logParser;
    private final Thread logParserThread;

    private final String partition;

    public GenericConnector(Config config, RealTimeProcessorMgmt realTimeProcessorMgmt, int genericConnectorPort, String partition) {

        super(genericConnectorPort, partition);

        this.partition = partition;

        logger.info(String.format("GenericConnector started on port [%d] for partion [%s]", genericConnectorPort,partition));

        logParser = new LogParser(partition, config, realTimeProcessorMgmt, partition);
        outputQueue = logParser.getInputQueue();
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

        outputQueue = null;
        logger.info(String.format("LogParser [%s] stopped",logParser.getName()));




    }


    public void run(Socket data) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(data.getInputStream()));

            InetAddress clientAddress = data.getInetAddress();
            int port = data.getPort();
            logger.info("Connected to client: " + clientAddress.getHostAddress() + ":" + port);


            String inputLine;

            while ( (! shouldStop) && (inputLine = in.readLine()) != null) {
                logger.debug(String.format("Client[%b]: [%s]", shouldStop, inputLine));
                try {
                    outputQueue.put(inputLine);
                } catch (InterruptedException e) {
                    if (! shouldStop)
                        logger.error("InterruptedException", e);
                }
            }

            in.close();

        } catch (IOException e) {
            logger.error("IOException", e);
        }
    }

}
