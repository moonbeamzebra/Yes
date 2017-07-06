package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.utils.QueueProcessor;
import ca.magenta.utils.Runner;
import ca.magenta.utils.queuing.StopWaitAsked;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Partition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Dispatcher extends QueueProcessor {

    public static final String SHORT_NAME = "DSPTCHR";
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final RealTimeProcessorMgmt realTimeProcessorMgmt;
    private final LongTermProcessorMgmt longTermProcessorMgmt;

    public Dispatcher(String name, Partition partition, MasterIndex masterIndex) {

        super(name, partition, Globals.getConfig().getDispatcherQueueDepth(), 650000);

        longTermProcessorMgmt =
                new LongTermProcessorMgmt(masterIndex,
                        LongTermProcessorMgmt.SHORT_NAME + "-" + partition.getInstanceName(),
                        Globals.getConfig().getLongTermCuttingTime(),
                        partition);

        realTimeProcessorMgmt = new RealTimeProcessorMgmt(RealTimeProcessorMgmt.SHORT_NAME + "-" + partition.getInstanceName(),
                Globals.getConfig().getRealTimeCuttingTime(),
                partition);

    }

    @Override
    public void run() {

        ObjectMapper mapper = new ObjectMapper();

        logger.info("New Dispatcher running for partition [{}]", partition.getInstanceName());
        count = 0;
        long startTime = System.currentTimeMillis();
        long previousNow = startTime;
        long queueLength;
        long hiWaterMarkQueueLength = 0;
        try {
            while (doRun) {
                queueLength = inputQueue.size();
                if (queueLength > hiWaterMarkQueueLength)
                    hiWaterMarkQueueLength = queueLength;
                try {
                    longTermProcessorMgmt.isEndDrainsCanDrain(this);
                    String jsonMsg = takeFromQueue();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Dispatcher received: {}", jsonMsg);
                    }
                    putInQueues(mapper, jsonMsg);

                    count++;

                    previousNow = printReport(startTime, previousNow, queueLength, hiWaterMarkQueueLength);
                }
                catch (StopWaitAsked e)
                {
                    if (doRun)
                    {
                        logger.error("Stop Wait Asked");
                    }
                }
            }
        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
        }

    }

    private long printReport(long startTime, long lastReportTimestamp, long queueLength, long hiWaterMarkQueueLength) {
        long newlastReportTimestamp = lastReportTimestamp;
        long now = System.currentTimeMillis();
        if ((count % printEvery) == 0) {
            long totalTime = now - lastReportTimestamp;

            long totalTimeSinceStart = now - startTime;
            float msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;


            float msgPerSec = ((float) printEvery / (float) totalTime) * 1000;

            String report = this.buildReportString(totalTime, msgPerSec, queueLength, hiWaterMarkQueueLength, msgPerSecSinceStart);

            System.out.println(report);

            newlastReportTimestamp = now;
        }

        return newlastReportTimestamp;

    }

    private void putInQueues(ObjectMapper mapper, String jsonMsg) throws InterruptedException {
        try {
            NormalizedMsgRecord normalizedMsgRecord = new NormalizedMsgRecord(mapper, jsonMsg, true);

            if (logger.isDebugEnabled()) {
                logger.debug("Before putInQueue");
            }
            longTermProcessorMgmt.putInQueue(normalizedMsgRecord);
            realTimeProcessorMgmt.putInQueue(normalizedMsgRecord);
        } catch (IOException e) {
            logger.error("IOException", e);
        }
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
    public boolean isEndDrainsCanDrain(Runner callerRunner) throws StopWaitAsked, InterruptedException {

        return isLocalQueueCanDrain(callerRunner) &&
                realTimeProcessorMgmt.isEndDrainsCanDrain(callerRunner) &&
                longTermProcessorMgmt.isEndDrainsCanDrain(callerRunner);

    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }
}
