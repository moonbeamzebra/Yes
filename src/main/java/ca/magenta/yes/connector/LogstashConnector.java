package ca.magenta.yes.connector;

import ca.magenta.utils.TCPServer;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.Dispatcher;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;


/**
 * Created by jean-paul.laberge on 12/19/2016.
 */
public class LogstashConnector extends TCPServer {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());


    private final BlockingQueue<String> dispatcherQueue;

    public LogstashConnector(Config config) {

        super(config.getLogstashConnectorPort(), LogstashConnector.class.getName());

        logger.info(String.format("LogstashConnector started on port [%d]", config.getLogstashConnectorPort()));

        Dispatcher dispatcher = new Dispatcher("Dispatcher", config, null, null);
        dispatcherQueue = dispatcher.getInputQueue();
        Thread dispatcherThread = new Thread(dispatcher, "Dispatcher");
        dispatcherThread.start();


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
                inputLine = inputLine.replace("\"tags\":[]", "\"tags\":\"[]\"");
                dispatcherQueue.put(inputLine);
            }

            in.close();
            data.close();

            // Process the data socket here.
        } catch (Exception e) {
            logger.error("", e);
        }
    }

}
