package ca.magenta.yes.stages;

import ca.magenta.utils.AppException;
import ca.magenta.utils.QueueProcessor;
import ca.magenta.utils.Runner;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.MasterIndexRecord;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Partition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class LongTermIndexPublisher extends QueueProcessor {

    private static final String SHORT_NAME = "LTIPub";
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final MasterIndex masterIndex;


    public void run() {

        logger.info(String.format("LongTermIndexPublisher start running for partition [%s]", partition.getInstanceName()));

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

        while (doRun) {
            queueLength = inputQueue.size();
            if (queueLength > hiWaterMarkQueueLength)
                hiWaterMarkQueueLength = queueLength;
            Package aPackage = null;
            try {
                aPackage = takeFromQueue();

                aPackage.luceneIndexWriter.commit();
                if (logger.isTraceEnabled())
                {
                    logger.trace("Index commited");
                }

                aPackage.luceneIndexWriter.close();
                if (logger.isTraceEnabled())
                {
                    logger.trace("Index closed");
                }


                String publishedFileName = NormalizedMsgRecord.forgePublishedFileName(aPackage.relativePath, partition, aPackage.runtimeTimestamps);
                String newIndexPathName = aPackage.indexPath + File.separator + publishedFileName;
                File dir = new File(aPackage.tempIndexPathName);
                File newDirName = new File(newIndexPathName);
                if (dir.isDirectory()) {
                    if (dir.renameTo(newDirName)) {
                        masterIndex.addRecord(new MasterIndexRecord(publishedFileName, partition.getName(), aPackage.runtimeTimestamps));
                        logger.info(String.format("Index [%s] published", publishedFileName));
                    } else {
                        logger.error(String.format("Cannot rename [%s to %s]", aPackage.tempIndexPathName, newIndexPathName));
                    }
                } else {
                    logger.error(String.format("Unexpected error; [%s] is not a directory", newIndexPathName));
                }
            } catch (InterruptedException e) {
                if (doRun)
                    logger.error("InterruptedException", e);
            } catch (AppException e) {
                logger.error("AppException", e);
            } catch (IOException e) {
                logger.error("IOException", e);
            }
            logger.debug("LongTermIndexPublisher received package");

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

        logger.info(String.format("LongTermIndexPublisher stop running for partition [%s]", partition.getInstanceName()));

    }

    private Package takeFromQueue() throws InterruptedException {

        Package aPackage = (Package) inputQueue.take();

        if (logger.isTraceEnabled())
        {
            logger.trace("Package taken");
        }

        return aPackage;
    }

    LongTermIndexPublisher(String name, Partition partition, MasterIndex masterIndex) {
        super(name, partition, 10, 20);

        this.masterIndex = masterIndex;
    }

    @Override
    public boolean isEndDrainsCanDrain(Runner callerRunner) {
        return isLocalQueueCanDrain(callerRunner);
    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }

    void putInQueue(Package publishPackage) throws InterruptedException {

        if (logger.isTraceEnabled())
        {
            logger.trace("Index publiched");
        }
        this.putInQueueImpl(publishPackage, Globals.getConfig().getQueueDepthWarningThreshold());
    }


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
