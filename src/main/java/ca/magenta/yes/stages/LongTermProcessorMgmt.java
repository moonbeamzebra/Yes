package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.utils.queuing.MyBlockingQueue;
import ca.magenta.utils.queuing.StopWaitAsked;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

class LongTermProcessorMgmt extends ProcessorMgmt {

    private static final Logger logger = LoggerFactory.getLogger(LongTermProcessorMgmt.class.getName());
    static final String SHORT_NAME = "LTPM";

    private final LongTermIndexPublisher longTermIndexPublisher;

    LongTermProcessorMgmt(MasterIndex masterIndex, String name, long cuttingTime, Partition partition) {
        super(name, partition,
                (new StringBuilder()).append(LongTermProcessor.SHORT_NAME).append('-').append(partition.getInstanceName()).toString(), cuttingTime);


        this.longTermIndexPublisher = new LongTermIndexPublisher(name, partition, masterIndex);
    }

    @Override
    protected long giveRandom() {
        return ThreadLocalRandom.current().nextInt(0, 5000);
    }

    synchronized void publishIndex(Processor longTermProcessor,
                                   String relativePath,
                                   String tempIndexPathName) throws IOException, AppException {

        LongTermIndexPublisher.Package publishPackage = new LongTermIndexPublisher.Package(
                longTermProcessor.luceneIndexWriter,
                Globals.getConfig().getLtIndexBaseDirectory(),
                relativePath,
                tempIndexPathName,
                longTermProcessor.getRuntimeTimestamps());

        try {
            longTermIndexPublisher.putInQueue(publishPackage);
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }
    }

    synchronized void deleteUnusedIndex(String indexPathName) {
        if (logger.isDebugEnabled())
        {
            logger.debug("Delete unused index [{}]", indexPathName);
        }

        File index = new File(indexPathName);
        String[] entries = index.list();
        if (entries != null) {
            for (String s : entries) {
                if (logger.isTraceEnabled()) logger.trace("Delete [{}]", s);
                File currentFile = new File(index.getPath(), s);
                if (currentFile.delete()) {
                    if (logger.isTraceEnabled()) logger.trace("Done");
                }
                else {
                    logger.error("Cannot delete [{}/{}]", index.getPath(), s);
                }
            }
        }
        if (logger.isTraceEnabled()) logger.trace("Delete dir [{}]", indexPathName);
        if (index.delete()) {
            if (logger.isTraceEnabled()) logger.trace("Done");
        }
        else {
            logger.error("Cannot delete [{}]", indexPathName);
        }
    }

    Processor createProcessor(MyBlockingQueue queue, int queueDepth) throws AppException {

        return new LongTermProcessor(this, partition, queue, queueDepth);

    }

    @Override
    public boolean isEndDrainsCanDrain(Runner callerRunner) throws InterruptedException, StopWaitAsked {

        return isLocalQueueCanDrain(callerRunner) && longTermIndexPublisher.isEndDrainsCanDrain(callerRunner);

    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }

    @Override
    public synchronized void startInstance() throws AppException {

        longTermIndexPublisher.startInstance();

        super.startInstance();
    }

    @Override
    public synchronized void stopInstance() {
        super.stopInstance();

        longTermIndexPublisher.letDrain();

        longTermIndexPublisher.gentlyStopInstance(2000);

        longTermIndexPublisher.stopInstance();

    }

}
