package ca.magenta.yes.connector;

import ca.magenta.utils.TCPServer;
import ca.magenta.yes.Config;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;


/**
 * Created by jean-paul.laberge on 12/19/2016.
 */
@Component
public class GenericConnector extends TCPServer {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());


    private final BlockingQueue<String> outputQueue;

    public GenericConnector(Config config) {

        super(config.getGenericConnectorPort(), GenericConnector.class.getName());

        logger.info(String.format("GenericConnector started on port [%d]", config.getGenericConnectorPort()));

        LogParser logParser = new LogParser("LogParser", config);
        outputQueue = logParser.getInputQueue();
        Thread logParserThread = new Thread(logParser, "LogParser");
        logParserThread.start();


    }

    public void run(Socket data) {
        try {



            InetAddress clientAddress = data.getInetAddress();
            int port = data.getPort();
            logger.info("Connected to client: " + clientAddress.getHostAddress() + ":" + port);

            BufferedReader in = new BufferedReader(new InputStreamReader(data.getInputStream()));

            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                logger.debug("Client: " + inputLine);
                outputQueue.put(inputLine);
            }

            in.close();
            data.close();

            // Process the data socket here.
        } catch (Exception e) {
            logger.error("", e);
        }
    }

}