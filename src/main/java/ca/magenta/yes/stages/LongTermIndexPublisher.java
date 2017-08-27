package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.utils.queuing.MyQueueProcessor;
import ca.magenta.utils.queuing.StopWaitAsked;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.MasterIndexRecord;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Partition;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class LongTermIndexPublisher extends MyQueueProcessor<LongTermIndexPublisher.Package> {

    private static final String SHORT_NAME = "LTIPub";
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final MasterIndex masterIndex;


    @Override
    public void run() {

        logger.info("LongTermIndexPublisher start running for partition [{}]", partition.getInstanceName());

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
            Package aPackage;
            try {
                aPackage = takeFromQueue();

                aPackage.luceneIndexWriter.commit();
                if (logger.isTraceEnabled()) {
                    logger.trace("Index committed");
                }

                aPackage.luceneIndexWriter.close();
                if (logger.isTraceEnabled()) {
                    logger.trace("Index closed");
                }

                String newIndexPath = aPackage.indexPath + File.separator + aPackage.relativePath;
                String publishedFileName = NormalizedMsgRecord.forgePublishedFileName(partition, aPackage.runtimeTimestamps);
                String newIndexPathName = newIndexPath + File.separator + publishedFileName;
                File tmpDirName = new File(aPackage.tempIndexPathName);
                File newDir = new File(newIndexPath);
                File newDirName = new File(newIndexPathName);
                logger.info("Moving/Copying [{}] to [{}]", tmpDirName.getAbsoluteFile(), newDir.getAbsoluteFile());
                FileUtils.moveToDirectory(tmpDirName, newDir, true);
                String oldNameNewPathStr = newIndexPath + File.separator + tmpDirName.getName();
                File oldNameNewPath = new File(oldNameNewPathStr);
                logger.info("Renaming [{}] to [{}]", oldNameNewPath.getAbsoluteFile(), newDirName.getAbsoluteFile());
                if (oldNameNewPath.renameTo(newDirName.getAbsoluteFile())) {

                    String todayAndPublishedFileName = aPackage.relativePath + File.separator + publishedFileName;
                    masterIndex.addRecord(new MasterIndexRecord(todayAndPublishedFileName, partition.getName(), aPackage.runtimeTimestamps));
                    logger.info("Index [{}] published in [{}]", todayAndPublishedFileName, aPackage.indexPath);
                } else {
                    logger.error("ERROR Renaming [{}] to [{}]", oldNameNewPath.getAbsoluteFile(), newDirName.getAbsoluteFile());
                }

            } catch (StopWaitAsked e) {
                if (isDoRun())
                    logger.error(e.getClass().getSimpleName(), e);
            } catch (InterruptedException e) {
                if (isDoRun())
                    logger.error(e.getClass().getSimpleName(), e);
            } catch (AppException | IOException e) {
                logger.error(e.getClass().getSimpleName(), e);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("LongTermIndexPublisher received package");
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
                previousNow = now;
            }
        }

        logger.info("LongTermIndexPublisher stop running for partition [{}]", partition.getInstanceName());

    }

//    private Package takeFromQueue() throws InterruptedException, StopWaitAsked {
//
//        Package aPackage = (Package) inputQueue.take();
//
//        if (logger.isTraceEnabled()) {
//            logger.trace("Package taken");
//        }
//
//        return aPackage;
//    }

    LongTermIndexPublisher(String name, Partition partition, MasterIndex masterIndex) {
        super(name, partition, 10, 20);

        this.masterIndex = masterIndex;
    }

    @Override
    public void waitWhileEndDrainsCanDrain(Runner callerRunner) throws InterruptedException, StopWaitAsked {

        waitWhileLocalQueueCanDrain();

    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }

//    void putInQueue(Package publishPackage) throws InterruptedException {
//
//        if (logger.isTraceEnabled()) {
//            logger.trace("Index published");
//        }
//        this.putIntoQueue(publishPackage);
//    }


    static class Package {

        private final IndexWriter luceneIndexWriter;
        private final String indexPath;
        private final String relativePath;
        private final String tempIndexPathName;
        private final MasterIndexRecord.RuntimeTimestamps runtimeTimestamps;

        Package(IndexWriter luceneIndexWriter, String indexPath, String relativePath, String tempIndexPathName, MasterIndexRecord.RuntimeTimestamps runtimeTimestamps) {
            this.luceneIndexWriter = luceneIndexWriter;
            this.indexPath = indexPath;
            this.relativePath = relativePath;
            this.tempIndexPathName = tempIndexPathName;
            this.runtimeTimestamps = runtimeTimestamps;

        }
    }
}
