package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.utils.queuing.MyQueueProcessor;
import ca.magenta.utils.queuing.StopWaitAsked;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.ParsedMessage;
import ca.magenta.yes.data.Partition;
import ca.magenta.yes.stages.Dispatcher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


public class LogParser extends MyQueueProcessor<String> {

    static final String SHORT_NAME = "LogP";
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final EffectiveMsgParser effectiveMsgParser;

    private final Dispatcher dispatcher;

    LogParser(String name, Partition partition, MasterIndex masterIndex) throws AppException {

        super(name, partition, Globals.getConfig().getLogParserQueueDepth(), 100000);

        effectiveMsgParser = getParserInstance();

        dispatcher = new Dispatcher(Dispatcher.SHORT_NAME + "-" + partition.getInstanceName(), partition, masterIndex);
    }

    public void run() {

        logger.info(String.format("LogParser start running for partition [%s]", partition.getInstanceName()));

        ObjectMapper objectMapper = new ObjectMapper();

        count = 0;
        long startTime = System.currentTimeMillis();
        long previousNow = System.currentTimeMillis();
        long now;
        long totalTime;
        long totalTimeSinceStart;
        float msgPerSec;
        float msgPerSecSinceStart;
        long queueLength;
        long hiWaterMarkQueueLength = 0;

        while (isDoRun()) {
            queueLength = inputQueue.size();
            if (queueLength > hiWaterMarkQueueLength)
                hiWaterMarkQueueLength = queueLength;
            try {
                dispatcher.waitWhileEndDrainsCanDrain(this);
                String logMsg = null;
                try {
                    logMsg = takeFromQueue();
                    try {
                        dispatchParsingAndProcessing(objectMapper, logMsg, dispatcher);
                    } catch (JsonProcessingException | AppException e) {
                        logger.error(e.getClass().getSimpleName(), e);
                    }
                } catch (InterruptedException e) {
                    if (isDoRun())
                        logger.error("InterruptedException", e);
                }
                logger.debug("LogParser received: " + logMsg);

                count++;

                if ((count % printEvery) == 0) {
                    now = System.currentTimeMillis();
                    totalTime = now - previousNow;

                    totalTimeSinceStart = now - startTime;
                    msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;

                    msgPerSec = ((float) printEvery / (float) totalTime) * 1000;

                    String report = this.buildReportString(totalTime, msgPerSec, queueLength, hiWaterMarkQueueLength, msgPerSecSinceStart);

                    System.out.println(report);
                    // System.out.println(printEvery + " messages sent in " + totalTime + " msec; [" + msgPerSec + " msgs/sec] in queue: " + inputQueue.size());
                    previousNow = now;
                }
            }
            catch (StopWaitAsked e )
            {
                if (isDoRun())
                    logger.warn("Stop Wait Asked");
            }
            catch (InterruptedException e )
            {
                if (isDoRun())
                    logger.error(e.getClass().getSimpleName(), e);
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


    private void dispatchParsingAndProcessing(ObjectMapper objectMapper, String msg, Dispatcher dispatcher) throws JsonProcessingException, InterruptedException, AppException {

        String msgType =  "pseudoCheckpoint";

        long receiveTime = System.currentTimeMillis();

        ParsedMessage parsedMessage = effectiveMsgParser.doParse(objectMapper, msg);

        HashMap<String, Object> hashedMsg = NormalizedMsgRecord.initiateMsgHash(receiveTime, msg, partition.getName(), parsedMessage);

        String jsonMsg = objectMapper.writeValueAsString(hashedMsg);

        dispatcher.putIntoQueue(jsonMsg);

    }

//    private String takeFromQueue() throws InterruptedException, StopWaitAsked {
//        return inputQueue.take();
//    }
//
//    void putInQueue(String message) throws InterruptedException {
//
//        this.putIntoQueue(message);
//    }

    @Override
    public void waitWhileEndDrainsCanDrain(Runner callerRunner) throws InterruptedException, StopWaitAsked {

        dispatcher.waitWhileEndDrainsCanDrain(callerRunner);

        this.waitWhileLocalQueueCanDrain(callerRunner);
    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }


    private EffectiveMsgParser getParserInstance() throws AppException {
        EffectiveMsgParser effectiveMsgParser = null;


        String parserClass = partition.getParserClass();
        if (parserClass == null) {
            effectiveMsgParser = new DefaultEffectiveMsgParser();
        } else {

            try {
                logger.info("Starting alternate Parser Class=[{}] ...", parserClass);

                effectiveMsgParser = (EffectiveMsgParser) Class.forName(parserClass).newInstance();

                logger.info("Started");

            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new AppException(e);
            }
        }

        return effectiveMsgParser;
    }

}
