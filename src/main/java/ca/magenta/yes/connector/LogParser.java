package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.utils.QueueProcessor;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.stages.Dispatcher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


public class LogParser extends QueueProcessor {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    private final Dispatcher dispatcher;

    LogParser(String name, String partition, MasterIndex masterIndex) {

        super(name, partition, Globals.getConfig().getLogParserQueueDepth(), 100000);

        dispatcher = new Dispatcher(this.partition, this.partition, masterIndex);
    }

    public void run() {

        logger.info(String.format("LogParser start running for partition [%s]", partition));

        count = 0;
        long previousNow = System.currentTimeMillis();
        long now;
        long totalTime;
        float msgPerSec;

        while (doRun) {
            if (dispatcher.isEndDrainsCanDrain()) {
                String logMsg = null;
                try {
                    logMsg = takeFromQueue();
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
            else
            {
                logger.warn(String.format("Partition:[%s] drains baddly", getName()));
            }
        }

        logger.info(String.format("LogParser stop running for partition [%s]", partition));

    }

    @Override
    public synchronized void startInstance() throws AppException {

        // Start drain
        dispatcher.startInstance();

        // Start source
        super.startInstance();
    }

    @Override
    public synchronized void stopInstance() {

        // Stop source
        super.stopInstance();

        dispatcher.letDrain();

        // Stop drain
        dispatcher.stopInstance();
    }


    private void dispatchParsingAndProcessing(String logMsg, Dispatcher dispatcher) throws JsonProcessingException, InterruptedException {

        String msgType =  "pseudoCheckpoint";

        HashMap<String, Object> hashedMsg = NormalizedMsgRecord.initiateMsgHash(logMsg, msgType, partition);

        // Let's parse pseudo Checkpoint logs
        // device=fw01|source=10.10.10.10|dest=20.20.20.20|port=80|action=drop
        String[] items = logMsg.split("\\|");

        for (String item : items) {
            if (logger.isDebugEnabled())
                logger.debug(String.format("ITEM:[%s]", item));
            int pos = item.indexOf("=");
            if (pos != -1) {
                String key = item.substring(0, pos);
                String value = item.substring(pos + 1);

                if (logger.isDebugEnabled())
                    logger.debug(String.format("KEY:[%s]; VALUE:[%s]", key, value));

                hashedMsg.put(key, value);

            }
        }


        ObjectMapper mapper = new ObjectMapper();

        String jsonMsg = mapper.writeValueAsString(hashedMsg);

        dispatcher.putInQueue(jsonMsg);

    }

    private String takeFromQueue() throws InterruptedException {
        return (String) inputQueue.take();
    }

    void putInQueue(String message) throws InterruptedException {

        this.putInQueueImpl(message, Globals.getConfig().getQueueDepthWarningThreshold());
    }

    @Override
    public boolean isEndDrainsCanDrain() {

        if (isLocalQueueCanDrain())
        {
            if (dispatcher.isEndDrainsCanDrain())
            {
                return true;
            }
        }

        return false;
    }
}
