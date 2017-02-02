package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;


public abstract class Processor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class.getPackage().getName());


    private final String name;
    final String partition;


    private BlockingQueue<HashMap<String, Object>> inputQueue;


    Directory indexDir;
    IndexWriter luceneIndexWriter;

    private volatile boolean doRun = true;

    private RunTimeStamps runTimeStamps;
    private final long startTime;
    private long hiWaterMarkQueueLength = 0;
    long previousNow = System.currentTimeMillis();

    private long count = 0;
    private long thisRunCount = 0;
    private long reportCount = 0;


    synchronized public void stopIt() {
        doRun = false;
    }

    private static final long printEvery = 100000;


    public Processor(String name, String partition, BlockingQueue<HashMap<String, Object>> inputQueue) throws AppException {

        this.name = name;
        this.partition = partition;
        this.inputQueue = inputQueue;

        this.startTime = System.currentTimeMillis();

    }

    public void run() {
        if (logger.isDebugEnabled())
            logger.debug(String.format("New [%s] running", this.getClass().getSimpleName()));
        doRun = true;
        thisRunCount = 0;
        reportCount = 0;
        runTimeStamps = new RunTimeStamps();
        try {

            while (doRun || !inputQueue.isEmpty()) {
                HashMap<String, Object> message = inputQueue.take();
                long srcTimestamp = Long.valueOf((String) message.get("srcTimestamp"));
                long rxTimestamp = Long.valueOf((String) message.get("rxTimestamp"));
                runTimeStamps.compute(srcTimestamp, rxTimestamp);
                if (logger.isDebugEnabled())
                    logger.debug("Processor received: " + message);
                try {
                    storeInLucene(message);
                    count++;
                    thisRunCount++;
                    reportCount++;

                } catch (AppException e) {
                    logger.error("AppException", e);
                }

                if (reportCount == printEvery) {
                    printReport();
                }
            }

        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
            else if (logger.isDebugEnabled())
                logger.debug("Processor manager asked to stop!");
        }
        runTimeStamps.setRunEndTimestamp(System.currentTimeMillis());
    }

    synchronized void commitAndClose() throws IOException {
        luceneIndexWriter.commit();
        luceneIndexWriter.close();

    }

    synchronized void printReport() {
        long queueLength = inputQueue.size();
        if (queueLength > hiWaterMarkQueueLength)
            hiWaterMarkQueueLength = queueLength;

        long now = System.currentTimeMillis();

        long totalTimeSinceStart = now - startTime;
        float msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;

        long totalTime = now - previousNow;
        float msgPerSec = ((float) reportCount / (float) totalTime) * 1000;

        System.out.println(partition + "-" + this.getClass().getSimpleName() + ": " + reportCount +
                " messages sent in " + totalTime +
                " msec; [" + msgPerSec + " msgs/sec] in queue: " + queueLength + "/" + hiWaterMarkQueueLength +
                " trend: [" + msgPerSecSinceStart + " msgs/sec] ");
        previousNow = now;
        reportCount = 0;

    }

    synchronized private void storeInLucene(HashMap<String, Object> message) throws AppException {

        try {

            Document document = new Document();

            //			<xs:element name="device" type="xs:string" />
            //			<xs:element name="source" type="xs:string" />
            //			<xs:element name="dest" type="xs:string" />
            //			<xs:element name="port" type="xs:string" />
            //			<xs:element name="action" type="xs:string" />
            //			<xs:element name="customer" type="xs:string" />
            //			<xs:element name="message" type="xs:string" />
            //			<xs:element name="type" type="xs:string" />
            for (Map.Entry<String, Object> fieldE : message.entrySet()) {
                if ("message".equals(fieldE.getKey())) {
                    document.add(new TextField("message", (String) fieldE.getValue(), Field.Store.YES));
                } else if (fieldE.getValue() instanceof Integer) {
                    document.add(new IntPoint(fieldE.getKey(), (Integer) fieldE.getValue()));
                    document.add(new StoredField(fieldE.getKey(), (Integer) fieldE.getValue()));
                } else if (fieldE.getValue() instanceof Long) {
                    document.add(new LongPoint(fieldE.getKey(), (Long) fieldE.getValue()));
                    document.add(new StoredField(fieldE.getKey(), (Long) fieldE.getValue()));
                } else {
                    FieldType newType = new FieldType();
                    newType.setTokenized(false);
                    newType.setStored(true);
                    newType.setIndexOptions(IndexOptions.DOCS);
                    Field f = new Field (fieldE.getKey(), (String) fieldE.getValue(), newType);
                    //document.add(f);
                    document.add(new StringField(fieldE.getKey(), (String) fieldE.getValue(), Field.Store.YES));
                }
            }

            luceneIndexWriter.addDocument(document);
            if (logger.isDebugEnabled())
                logger.debug("Document added");
        } catch (IOException e) {
            throw new AppException("IOException", e);
        }


    }

    public void setInputQueue(BlockingQueue<HashMap<String, Object>> inputQueue) {
        this.inputQueue = inputQueue;
    }

    public abstract void createIndex(String indexPath) throws AppException;

    public RunTimeStamps getRunTimeStamps() {
        return runTimeStamps;
    }

    public long getThisRunCount() {
        return thisRunCount;
    }

    synchronized public Directory getIndexDir() {
        return indexDir;
    }

    static class RunTimeStamps {

        private long olderSrcTimestamp;
        private long newerSrcTimestamp;

        private long olderRxTimestamp;
        private long newerRxTimestamp;

        private final long runStartTimestamp;
        private long runEndTimestamp;

        RunTimeStamps() {
            runStartTimestamp = System.currentTimeMillis();

            olderSrcTimestamp = Long.MAX_VALUE;
            newerSrcTimestamp = 0;

            olderRxTimestamp = Long.MAX_VALUE;
            newerRxTimestamp = 0;
        }

        public void compute(long srcTimestamp, long rxTimestamp) {

            if (srcTimestamp < olderSrcTimestamp)
                olderSrcTimestamp = srcTimestamp;
            if (srcTimestamp > newerSrcTimestamp)
                newerSrcTimestamp = srcTimestamp;

            if (rxTimestamp < olderRxTimestamp)
                olderRxTimestamp = rxTimestamp;
            if (rxTimestamp > newerRxTimestamp)
                newerRxTimestamp = rxTimestamp;
        }

        public long getOlderSrcTimestamp() {
            return olderSrcTimestamp;
        }

        public long getNewerSrcTimestamp() {
            return newerSrcTimestamp;
        }

        public long getOlderRxTimestamp() {
            return olderRxTimestamp;
        }

        public long getNewerRxTimestamp() {
            return newerRxTimestamp;
        }

        public long getRunStartTimestamp() {
            return runStartTimestamp;
        }

        public long getRunEndTimestamp() {
            return runEndTimestamp;
        }

        public void setRunEndTimestamp(long runEndTimestamp) {
            this.runEndTimestamp = runEndTimestamp;
        }
    }

}
