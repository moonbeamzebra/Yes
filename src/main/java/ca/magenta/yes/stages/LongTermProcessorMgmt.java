package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.MasterIndexRecord;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

class LongTermProcessorMgmt extends ProcessorMgmt {

    private static final Logger logger = LoggerFactory.getLogger(LongTermProcessorMgmt.class.getName());
    public static final String SHORT_NAME = "LTPM";

    private final MasterIndex masterIndex;

    private final LongTermIndexPublisher longTermIndexPublisher;

    LongTermProcessorMgmt(MasterIndex masterIndex, String name, long cuttingTime, Partition partition) {
        super(name, partition,
                (new StringBuilder()).append(LongTermProcessor.SHORT_NAME).append('-').append(partition.getInstanceName()).toString(),cuttingTime);

        this.masterIndex = masterIndex;


        this.longTermIndexPublisher = new LongTermIndexPublisher(name, partition,masterIndex);
    }

    @Override
    protected long giveRandom() {
        return ThreadLocalRandom.current().nextInt(0, 5000);
    }

    synchronized void publishIndex(Processor longTermProcessor,
                                   String indexPath,
                                   String relativePath,
                                   String tempIndexPathName) throws IOException, AppException {

        LongTermIndexPublisher.Package publishPackage = new LongTermIndexPublisher.Package(
                longTermProcessor.luceneIndexWriter,
                indexPath,
                relativePath,
                tempIndexPathName,
                longTermProcessor.getRuntimeTimestamps());

        try {
            longTermIndexPublisher.putInQueue(publishPackage);
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }


//        String publishedFileName = NormalizedMsgRecord.forgePublishedFileName(relativePath, partition, longTermProcessor.getRuntimeTimestamps());
//        String newIndexPathName = indexPath + File.separator + publishedFileName;
//        File dir = new File(tempIndexPathName);
//        File newDirName = new File(newIndexPathName);
//        if (dir.isDirectory()) {
//            if (dir.renameTo(newDirName)) {
//                masterIndex.addRecord(new MasterIndexRecord(publishedFileName, partition.getName(), longTermProcessor.getRuntimeTimestamps()));
//                logger.info(String.format("Index [%s] published", publishedFileName));
//            } else {
//                logger.error(String.format("Cannot rename [%s to %s]", tempIndexPathName, newIndexPathName));
//            }
//        } else {
//            logger.error(String.format("Unexpected error; [%s] is not a directory", newIndexPathName));
//        }
    }

    synchronized void deleteUnusedIndex(String indexPathName) {
        logger.debug(String.format("Delete unused index [%s]", indexPathName));

        File index = new File(indexPathName);
        String[] entries = index.list();
        if (entries != null) {
            for (String s : entries) {
                logger.trace(String.format("Delete [%s]", s));
                File currentFile = new File(index.getPath(), s);
                if (currentFile.delete())
                    logger.trace("Done");
                else
                    logger.error(String.format("Cannot delete [%s/%s]", index.getPath(), s));
            }
        }
        logger.trace(String.format("Delete dir [%s]", indexPathName));
        if (index.delete())
            logger.trace("Done");
        else
            logger.error(String.format("Cannot delete [%s]", indexPathName));
    }

    Processor createProcessor(BlockingQueue<Object> queue, int queueDepth) throws AppException {

        return new LongTermProcessor(partition, queue, queueDepth);

    }

    @Override
    public boolean isEndDrainsCanDrain(Runner callerRunner) {

        if (isLocalQueueCanDrain(callerRunner))
        {
            return longTermIndexPublisher.isEndDrainsCanDrain(callerRunner);
        }

        return false;
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
