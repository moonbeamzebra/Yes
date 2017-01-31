package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.Dispatcher;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class LogParser extends Runner {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());


    private BlockingQueue<String> inputQueue;
    private final Config config;
    private final RealTimeProcessorMgmt realTimeProcessorMgmt;
    private final Dispatcher dispatcher;


    private final String partition;

    private static final long printEvery = 100000;

    private long count = 0;

    public LogParser(String name, Config config, RealTimeProcessorMgmt realTimeProcessorMgmt, String partition) {

        super(name);

        this.realTimeProcessorMgmt = realTimeProcessorMgmt;

        this.inputQueue = new ArrayBlockingQueue<String>(config.getLogParserQueueDepth());

        this.config = config;

        this.partition = partition;

        dispatcher = new Dispatcher(this.partition, config, realTimeProcessorMgmt, this.partition);

    }

    public void run() {

        logger.info(String.format("LogParser start running for partition [%s]", partition));

        count = 0;
        long previousNow = System.currentTimeMillis();
        long now;
        long totalTime;
        float msgPerSec;

        while (doRun) {
            String logMsg = null;
            try {
                logMsg = inputQueue.take();
                try {
                    dispatchParsingAndProcessing(logMsg, dispatcher);
                } catch (JsonProcessingException e) {
                    logger.error("JsonProcessingException", e);
                }
            } catch (InterruptedException e) {
                if (doRun)
                    logger.error("InterruptedException", e);
            }
            logger.debug("LogParser received: " + logMsg);

            count++;

            if ((count % printEvery) == 0) {
                now = System.currentTimeMillis();
                totalTime = now - previousNow;

                msgPerSec = ((float) printEvery / (float) totalTime) * 1000;

                System.out.println(printEvery + " messages sent in " + totalTime + " msec; [" + msgPerSec + " msgs/sec] in queue: " + inputQueue.size());
                previousNow = now;
            }
        }

        logger.info(String.format("LogParser stop running for partition [%s]", partition));

    }

    @Override
    public synchronized void startInstance() throws AppException {

        dispatcher.startInstance();
        super.startInstance();
    }

    @Override
    public synchronized void stopInstance() {

        super.stopInstance();

        dispatcher.stopInstance();
    }


    private void dispatchParsingAndProcessing(String logMsg, Dispatcher dispatcher) throws JsonProcessingException, InterruptedException {

        // device=fw01|source=10.10.10.10|dest=20.20.20.20|port=80|action=drop

        HashMap<String, Object> hashedMsg = new HashMap<String, Object>();

        hashedMsg.put("message", logMsg);
        hashedMsg.put("type", "pseudoCheckpoint");
        hashedMsg.put("partition", partition);

        String[] items = logMsg.split("\\|");

        for (String item : items) {
            //logger.info(String.format("ITEM:[%s]", item));
            int pos = item.indexOf("=");
            if (pos != -1) {
                String key = item.substring(0, pos);
                String value = item.substring(pos + 1);

                //logger.info(String.format("KEY:[%s]; VALUE:[%s]", key, value));

                hashedMsg.put(key, value);

            }
        }


        ObjectMapper mapper = new ObjectMapper();

        String jsonMsg = mapper.writeValueAsString(hashedMsg);

        dispatcher.putInQueue(jsonMsg);

    }

    public void putInQueue(String message) throws InterruptedException {

        inputQueue.put(message);

        if (logger.isWarnEnabled()) {
            int length = inputQueue.size();
            float percentFull = length / config.getLogParserQueueDepth();

            if (percentFull > config.getQueueDepthWarningThreshold())
                logger.warn(String.format("Queue length threashold bypassed max:[%d]; " +
                                "queue length:[%d] " +
                                "Percent:[%f] " +
                                "Threshold:[%f]",
                        config.getLogParserQueueDepth(), length, percentFull, config.getQueueDepthWarningThreshold()));
        }

    }
}
