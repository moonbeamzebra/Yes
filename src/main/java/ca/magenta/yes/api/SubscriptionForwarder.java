package ca.magenta.yes.api;

import ca.magenta.yes.connector.common.IndexSubscriber;
import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;

import java.io.PrintWriter;

/**
 * @author jean-paul.laberge <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-12-07
 */
public class SubscriptionForwarder extends IndexSubscriber {
	

	public static Logger logger = Logger.getLogger(SubscriptionForwarder.class);
	
	private PrintWriter client = null;

    public SubscriptionForwarder(String name, String searchString, PrintWriter client) {
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
