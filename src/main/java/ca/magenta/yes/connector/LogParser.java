package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.Dispatcher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class LogParser implements Runnable {

	private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());

	private final String name;
	private final BlockingQueue<String> outputQueue;
	private final BlockingQueue<String> inputQueue;

	private final String customer;

    private volatile boolean doRun = true;

    public void stopIt() {
        doRun = false;
    }

    private static final long printEvery = 100000;

    private long count = 0;

    public LogParser(String name, Config config) {

		this.inputQueue = new ArrayBlockingQueue<String>(1000000);
		this.name = name;

		customer = Integer.toString(config.getGenericConnectorPort());

		Dispatcher dispatcher = new Dispatcher("Dispatcher", config);
		outputQueue = dispatcher.getInputQueue();
		Thread dispatcherThread = new Thread(dispatcher, "Dispatcher");
		dispatcherThread.start();

	}

    public void run() {
    	
    	logger.info("New LogParser running");
    	count = 0;
    	long previousNow = System.currentTimeMillis();
    	long now;
    	long totalTime;
    	float msgPerSec;
        try {

			while (doRun || ! inputQueue.isEmpty())
        	{
        		String logMsg = inputQueue.take();
				logger.debug("LogParser received: " + logMsg);
				try {
					dispatchParsingAndProcessing(logMsg);
				} catch (JsonProcessingException e) {
					logger.error("JsonProcessingException", e);
				}

				count++;
        		
        		if ( (count % printEvery) == 0)
        		{
        			now = System.currentTimeMillis();
        			totalTime = now - previousNow;
    				
    				msgPerSec = ((float)printEvery / (float)totalTime) * 1000;

    				System.out.println(printEvery + " messages sent in " + totalTime + " msec; [" + msgPerSec + " msgs/sec] in queue: " + inputQueue.size());
    				previousNow = now;
        		}
        	}
        } catch (InterruptedException e) {
        	logger.error("InterruptedException", e);
        }
    }

	private void dispatchParsingAndProcessing(String logMsg) throws JsonProcessingException, InterruptedException {

		// device=fw01|source=10.10.10.10|dest=20.20.20.20|port=80|action=drop

		HashMap<String, Object> hashedMsg = new HashMap<String, Object>();

		hashedMsg.put("message", logMsg);
		hashedMsg.put("type", "pseudoCheckpoint");
		hashedMsg.put("customer", customer);

		String[] items = logMsg.split("\\|");

		for (String item : items)
		{
			//logger.info(String.format("ITEM:[%s]", item));
			int pos = item.indexOf("=");
			if (pos != -1)
			{
				String key = item.substring(0, pos);
				String value = item.substring(pos+1);

				//logger.info(String.format("KEY:[%s]; VALUE:[%s]", key, value));

				hashedMsg.put(key, value);

			}
		}


		ObjectMapper mapper = new ObjectMapper();

		String jsonMsg = mapper.writeValueAsString(hashedMsg);

		outputQueue.put(jsonMsg);

	}

	public BlockingQueue<String> getInputQueue() {
		return inputQueue;
	}

}
