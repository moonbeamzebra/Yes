package ca.magenta.yes.stages;


import ca.magenta.utils.ThreadRunnable;
import ca.magenta.yes.Config;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class Dispatcher extends ThreadRunnable {

    private static final long printEvery = 650000;
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());
    private final String name;
    private final String partition;
    private final BlockingQueue<String> inputQueue;
    private final Config config;
    private long count = 0;
    private RealTimeProcessorMgmt realTimeProcessorMgmt;

    public Dispatcher(String name, Config config, RealTimeProcessorMgmt realTimeProcessorMgmt, String partition) {

        super(name);

        this.realTimeProcessorMgmt = realTimeProcessorMgmt;

        this.inputQueue = new ArrayBlockingQueue<String>(config.getDispatcherQueueDepth());
        this.name = name;

        this.partition = partition;

        this.config = config;
    }

    public void run() {

        ObjectMapper mapper = new ObjectMapper();

        LongTermProcessorMgmt longTermProcessorMgmt =
                new LongTermProcessorMgmt("LongTermProcessorMgmt",
                        config.getLongTermCuttingTime(),
                        config,
                        partition);
        longTermProcessorMgmt.startInstance();

//        RealTimeProcessorMgmt realTimeProcessorMgmt =
//                new RealTimeProcessorMgmt("RealTimeProcessorMgmt",
//                        config.getRealTimeCuttingTime(),
//                        config);
//        Thread realTimeThread = new Thread(realTimeProcessorMgmt, "RealTimeProcessorMgmt");
//        realTimeThread.start();

        logger.info(String.format("New Dispatcher running for partition [%s]", partition));
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
                String jsonMsg = inputQueue.take();
                logger.debug("Dispatcher received: " + jsonMsg);
                HashMap<String, Object> hashedMsg = null;
                try {

                    hashedMsg = mapper.readValue(jsonMsg, HashMap.class);
                    logger.debug("hashMsg received OBJ: " + hashedMsg.toString());


                    long epoch = System.currentTimeMillis();
                    hashedMsg.put("rxTimestamp", Long.toString(epoch));
                    String str = (String) hashedMsg.get("@timestamp");
                    if (str != null) {
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                        Date date = null;
                        date = df.parse(str);
                        epoch = date.getTime();
                        //logger.info(String.format("timestamp:[%d]", epoch));
                    }
                    hashedMsg.put("srcTimestamp", Long.toString(epoch));

                    longTermProcessorMgmt.putInQueue(hashedMsg);
                    realTimeProcessorMgmt.putInQueue(hashedMsg);
                } catch (ParseException e) {
                    logger.error("ParseException", e);
                } catch (IOException e) {
                    logger.error("IOException", e);
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

                    System.out.println(partition + "-" + "Dispatcher: " + printEvery +
                            " messages sent in " + totalTime +
                            " msec; [" + msgPerSec + " msgs/sec] in queue: " + queueLength + "/" + maxQueueLength +
                            " trend: [" + msgPerSecSinceStart + " msgs/sec] ");
                    previousNow = now;
                }
            }
        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
        }

        longTermProcessorMgmt.stopInstance();
    }

    public BlockingQueue<String> getInputQueue() {
        return inputQueue;
    }

    public void putInQueue(String jsonMsg) throws InterruptedException {

        inputQueue.put(jsonMsg);

        if (logger.isWarnEnabled()) {
            int length = inputQueue.size();
            float percentFull = length / config.getDispatcherQueueDepth();

            if (percentFull > config.getQueueDepthWarningThreshold())
                logger.warn(String.format("Queue length threashold bypassed max:[%d]; " +
                                "queue length:[%d] " +
                                "Percent:[%f] " +
                                "Threshold:[%f]",
                        config.getDispatcherQueueDepth(), length, percentFull, config.getQueueDepthWarningThreshold()));
        }

    }



}
