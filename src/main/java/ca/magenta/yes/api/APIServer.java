package ca.magenta.yes.api;

import ca.magenta.utils.TCPServer;
import ca.magenta.utils.TimeRange;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;

/**
 * @author jean-paul.laberge <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-11-25
 */
public class APIServer extends TCPServer {

	private static Logger logger = Logger.getLogger(APIServer.class);
	
    private volatile boolean doRun = true;
    private final String indexBaseDirectory;

    public void stop() {
        doRun = false;
    }

	public APIServer(int port, String name, String indexBaseDirectory) {

    	super(port, name);
    	this.indexBaseDirectory = indexBaseDirectory;
	}

	public void run(Socket data) {
		try {

			String apiServerName = this.getClass().getSimpleName() + "-" + this.getClientCount();
			String threadName = SubscriptionForwarder.class.getSimpleName() + "-" + this.getClientCount();

			InetAddress clientAddress = data.getInetAddress();
			int port = data.getPort();
			logger.info(apiServerName + " is now connected to client: " + clientAddress.getHostAddress() + ":" + port);

			PrintWriter toClient = new PrintWriter(data.getOutputStream(), true);
			
			BufferedReader fromClient = new BufferedReader(new InputStreamReader(data.getInputStream()));

			String inputLine;

			String mode = null;

			String searchString = "*";
			HashMap<String, String> control = null;
			if ((inputLine = fromClient.readLine()) != null) {
				logger.info("Client: " + inputLine);
				ObjectMapper mapper = new ObjectMapper();
				control = mapper.readValue(inputLine, HashMap.class);

				mode = control.get("mode");
				logger.info("Mode: " + mode);

			}

			if ("realTime".equals(mode))
			{
				searchString = control.get("searchString");
				logger.info("searchString: " + searchString);

				SubscriptionForwarder subscriptionServer = new SubscriptionForwarder(threadName, searchString, toClient);

				Thread subscritionServerThread = new Thread(subscriptionServer, threadName);
				subscritionServerThread.start();

				while ((inputLine = fromClient.readLine()) != null) {
					logger.info("Client: " + inputLine);

				}

				logger.info(apiServerName + " is now disconnected from client: " + clientAddress.getHostAddress() + ":" + port);

				fromClient.close();
				toClient.close();
				data.close();

				subscriptionServer.stop();
				subscritionServerThread.interrupt();
				subscritionServerThread.join();

				logger.info(threadName + " stopped");

				setClientCount(getClientCount() - 1);

			}
			else if ("longTerm".equals(mode))
			{
				String olderTimeStr = control.get("olderTime");
				logger.info("olderTimeStr: " + olderTimeStr);
				String newerTimeStr = control.get("newerTime");
				logger.info("newerTimeStr: " + newerTimeStr);

				TimeRange periodTimeRange = new TimeRange(Long.valueOf(olderTimeStr), Long.valueOf(newerTimeStr));

				searchString = control.get("searchString");
				logger.info("searchString: " + searchString);

				LongTermReader longTermReader = new LongTermReader(threadName,
						indexBaseDirectory,
						periodTimeRange,
						searchString,
						toClient);

				Thread longTermReaderThread = new Thread(longTermReader, threadName);
				longTermReaderThread.start();

				while ((inputLine = fromClient.readLine()) != null) {
					logger.info("Client: " + inputLine);

				}

				logger.info(apiServerName + " is now disconnected from client: " + clientAddress.getHostAddress() + ":" + port);

				fromClient.close();
				toClient.close();
				data.close();

				longTermReader.stop();
				longTermReaderThread.interrupt();
				longTermReaderThread.join();

				logger.info(threadName + " stopped");

				setClientCount(getClientCount() - 1);

			}


			// Process the data socket here.
		} catch (Exception e) {
			logger.error("", e);
		}
	}

}
