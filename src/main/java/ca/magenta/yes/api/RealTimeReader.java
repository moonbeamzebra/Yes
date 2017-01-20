package ca.magenta.yes.api;

import ca.magenta.yes.connector.common.IndexSubscriber;
import org.apache.log4j.Logger;

import java.io.PrintWriter;

/**
 * @author jean-paul.laberge <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-12-07
 */
public class RealTimeReader extends IndexSubscriber {
	

	public static Logger logger = Logger.getLogger(RealTimeReader.class);
	
	private PrintWriter client = null;

    public RealTimeReader(String name, String searchString, PrintWriter client) {
    	super(name, searchString);
    	this.client = client;
    }

	@Override
	protected void forward(String message) {

		if (client != null) {
			logger.debug("Out to client");
			client.println(message);
		}
	}

}
