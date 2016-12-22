package ca.magenta.yes.connector;

import ca.magenta.utils.TCPServer;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.Dispatcher;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/**
 * Created by jean-paul.laberge on 12/19/2016.
 */
@Component
public class TcpConnector extends TCPServer {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());


    private final BlockingQueue<String> dispatcherQueue;

    public TcpConnector(Config config) {

        super(config.getTcpConnectorPort(), TcpConnector.class.getName());

        logger.info(String.format("TcpConnector started on port [%d]", config.getTcpConnectorPort()));

        Dispatcher dispatcher = new Dispatcher("Dispatcher", config);
        dispatcherQueue = dispatcher.getInputQueue();
        Thread dispatcherThread = new Thread(dispatcher);
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
                inputLine = inputLine.replace("@timestamp", "logstasHtimestamp");
                inputLine = inputLine.replace("@version", "logstasHversion");
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
