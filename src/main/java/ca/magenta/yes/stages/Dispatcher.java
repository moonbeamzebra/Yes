package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.utils.QueueProcessor;
import ca.magenta.utils.Runner;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Partition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class Dispatcher extends QueueProcessor {

    public static final String SHORT_NAME = "DSPTCHR";
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final RealTimeProcessorMgmt realTimeProcessorMgmt;
    private final LongTermProcessorMgmt longTermProcessorMgmt;

    public Dispatcher(String name, Partition partition, MasterIndex masterIndex ) {

        super(name, partition, Globals.getConfig().getDispatcherQueueDepth(), 650000);

        longTermProcessorMgmt =
                new LongTermProcessorMgmt(masterIndex,
                        LongTermProcessorMgmt.SHORT_NAME + "-" + partition.getInstanceName(),
                        Globals.getConfig().getLongTermCuttingTime(),
                        partition);

        realTimeProcessorMgmt = new RealTimeProcessorMgmt(RealTimeProcessorMgmt.SHORT_NAME + "-"+partition.getInstanceName(),
                Globals.getConfig().getRealTimeCuttingTime(),
                partition);

    }

    public void run() {

        ObjectMapper mapper = new ObjectMapper();

        logger.info(String.format("New Dispatcher running for partition [%s]", partition.getInstanceName()));
        count = 0;
        long startTime = System.currentTimeMillis();
        long previousNow = startTime;
        long now;
        long totalTime;
        long totalTimeSinceStart;
        float msgPerSec;
        float msgPerSecSinceStart;
        long queueLength;
        long hiWaterMarkQueueLength = 0;
        try {
            while (doRun) {
                queueLength = inputQueue.size();
                if (queueLength > hiWaterMarkQueueLength)
                    hiWaterMarkQueueLength = queueLength;
                if (longTermProcessorMgmt.isEndDrainsCanDrain(this)) {
                    String jsonMsg = takeFromQueue();
                    logger.debug("Dispatcher received: " + jsonMsg);
                    HashMap<String, Object> hashedMsg;
                    try {
                        NormalizedMsgRecord normalizedMsgRecord = new NormalizedMsgRecord(mapper, jsonMsg, true);
                        //                    hashedMsg = mapper.readValue(jsonMsg, HashMap.class);
                        //                    logger.debug("hashMsg received OBJ: " + hashedMsg.toString());
                        //
                        //                    NormalizedMsgRecord.initiateTimestampsInMsgHash(hashedMsg);

                        if (logger.isDebugEnabled())
                            logger.debug("Before putInQueue");
                        longTermProcessorMgmt.putInQueue(normalizedMsgRecord);
                        realTimeProcessorMgmt.putInQueue(normalizedMsgRecord);
                    } catch (IOException e) {
                        logger.error("IOException", e);
                    }

                    count++;

                    if ((count % printEvery) == 0) {
                        now = System.currentTimeMillis();
                        totalTime = now - previousNow;

                        totalTimeSinceStart = now - startTime;
                        msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;


                        msgPerSec = ((float) printEvery / (float) totalTime) * 1000;

                        String report = this.buildReportString(totalTime, msgPerSec, queueLength, hiWaterMarkQueueLength, msgPerSecSinceStart);

                        System.out.println(report);
//                        System.out.println(partition + "-" + "Dispatcher: " + printEvery +
//                                " messages sent in " + totalTime +
//                                " msec; [" + msgPerSec + " msgs/sec] in queue: " + queueLength + "/" + hiWaterMarkQueueLength +
//                                " trend: [" + msgPerSecSinceStart + " msgs/sec] ");
                        previousNow = now;
                    }
                }
                else
                {
                    if (doRun)
                        logger.warn(String.format("Partition:[%s] drains baddly", getName()));
                }
            }
        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
        }

        //longTermProcessorMgmt.stopInstance();
    }

    public void putInQueue(String jsonMsg) throws InterruptedException {

        this.putInQueueImpl(jsonMsg, Globals.getConfig().getQueueDepthWarningThreshold());
    }

    private String takeFromQueue() throws InterruptedException {
        return (String) inputQueue.take();
    }

    @Override
    public synchronized void startInstance() throws AppException {

        longTermProcessorMgmt.startInstance();
        realTimeProcessorMgmt.startInstance();

        super.startInstance();
    }

    @Override
    public synchronized void stopInstance() {
        super.stopInstance();

        realTimeProcessorMgmt.stopInstance();
        longTermProcessorMgmt.stopInstance();

    }

    @Override
    public boolean isEndDrainsCanDrain(Runner callerRunner) {

        if (isLocalQueueCanDrain(callerRunner))
        {
            if (realTimeProcessorMgmt.isEndDrainsCanDrain(callerRunner)) {
                if (longTermProcessorMgmt.isEndDrainsCanDrain(callerRunner)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }
}
