package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.Config;
import ca.magenta.yes.data.LogstashMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class Dispatcher implements Runnable {

    private static final long printEvery = 650000;
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());
    private final String name;
    private final BlockingQueue<String> inputQueue;
    private final Config config;
    private volatile boolean doRun = true;
    private long count = 0;

    public Dispatcher(String name, Config config) {

        this.inputQueue = new ArrayBlockingQueue<String>(1000000);
        this.name = name;

        this.config = config;
    }

    public void stopIt() {
        doRun = false;
    }

    public void run() {

        ObjectMapper mapper = new ObjectMapper();

        LongTermProcessorMgmt longTermProcessorMgmt =
                new LongTermProcessorMgmt("LongTermProcessor",
                        config.getLongTermCuttingTime(),
                        config);
        BlockingQueue<LogstashMessage> longTermQueue = longTermProcessorMgmt.getInputQueue();
        Thread longTermThread = new Thread(longTermProcessorMgmt);
        longTermThread.start();

        logger.info("New Dispatcher running");
        count = 0;
        long startTime = System.currentTimeMillis();
        long previousNow = startTime;
        long now;
        long totalTime;
        long totalTimeSinceStart;
        float msgPerSec;
        float msgPerSecSinceStart;
        long queueLength = 0;
        long maxQueueLength = 0;
        try {

            while (doRun || !inputQueue.isEmpty()) {
                String message = inputQueue.take();
                logger.debug("Dispatcher received: " + message);
                try {
                    LogstashMessage logstashMessage = null;
                    try {
                        logstashMessage = mapper.readValue(message, LogstashMessage.class);
                        //2016-12-21T13:30:24.640Z
                        //yyyy-MM-ddTHH:mm:ss.SSSZ

//						String input = logstashMessage.getLogstasHtimestamp();
//						TimeZone utc = TimeZone.getTimeZone("UTC");
//						SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//						f.setTimeZone(utc);
//						GregorianCalendar cal = new GregorianCalendar(utc);
//						cal.setTime(f.parse(input));
//						System.out.println(cal.getTime());

                        String str = logstashMessage.getLogstasHtimestamp();
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                        Date date = null;
                        date = df.parse(str);
                        long epoch = date.getTime();
                        logstashMessage.setTimestamp(epoch);
                        String jsonInString = mapper.writeValueAsString(logstashMessage);
                        logger.debug("Dispatcher received OBJ: " + jsonInString);

                        longTermQueue.put(logstashMessage);
                    } catch (ParseException e) {
                        logger.error("ParseException", e);
                    } catch (IOException e) {
                        logger.error("IOException", e);
                    }
                } catch (AppException e) {
                    logger.error("ServiceException", e);
                }

                count++;
                queueLength = inputQueue.size();
                if (queueLength > maxQueueLength)
                    maxQueueLength = queueLength;


                if ((count % printEvery) == 0) {
                    now = System.currentTimeMillis();
                    totalTime = now - previousNow;

                    totalTimeSinceStart = now - startTime;
                    msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;


                    msgPerSec = ((float) printEvery / (float) totalTime) * 1000;

                    System.out.println("Dispatcher: " + printEvery +
                            " messages sent in " + totalTime +
                            " msec; [" + msgPerSec + " msgs/sec] in queue: " + queueLength + "/" + maxQueueLength +
                            " trend: [" + msgPerSecSinceStart + " msgs/sec] ");
                    previousNow = now;
                }
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }
    }

    public BlockingQueue<String> getInputQueue() {
        return inputQueue;
    }


}
