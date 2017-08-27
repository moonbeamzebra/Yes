package ca.magenta.yes.api;

import ca.magenta.yes.connector.common.IndexSubscriber;
import org.apache.log4j.Logger;

import java.io.PrintWriter;

public class RealTimeReader extends IndexSubscriber {


    public static final Logger logger = Logger.getLogger(RealTimeReader.class);

    private PrintWriter client = null;

    RealTimeReader(String name, String searchString, PrintWriter client) {
        super(name, searchString);
        this.client = client;
    }

    @Override
    protected void forward(String message) {

        if (client != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Out to client");
            }
            client.println(message);
        }
    }

}
