package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.MasterIndexRecord;
import ca.magenta.yes.data.NormalizedMsgRecord;
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

    LongTermProcessorMgmt(MasterIndex masterIndex, String name, long cuttingTime, String partition) {
        super(name, partition, (new StringBuilder()).append(LongTermProcessor.SHORT_NAME).append('-').append(partition).toString(),cuttingTime);

        this.masterIndex = masterIndex;
    }

    @Override
    protected long giveRandom() {
        return ThreadLocalRandom.current().nextInt(0, 5000);
    }

    synchronized void publishIndex(Processor longTermProcessor,
                                   String indexPath,
                                   String relativePath,
                                   String indexPathName) throws IOException, AppException {
        String publishedFileName = NormalizedMsgRecord.forgePublishedFileName(relativePath, partition, longTermProcessor.getRuntimeTimestamps());
        String newIndexPathName = indexPath + File.separator + publishedFileName;
        File dir = new File(indexPathName);
        File newDirName = new File(newIndexPathName);
        if (dir.isDirectory()) {
            if (dir.renameTo(newDirName)) {
                masterIndex.addRecord(new MasterIndexRecord(publishedFileName, partition, longTermProcessor.getRuntimeTimestamps()));
                logger.info(String.format("Index [%s] published", publishedFileName));
            } else {
                logger.error(String.format("Cannot rename [%s to %s]", indexPathName, newIndexPathName));
            }
        } else {
            logger.error(String.format("Unexpected error; [%s] is not a directory", newIndexPathName));
        }
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
        return isLocalQueueCanDrain(callerRunner);
    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }
}
