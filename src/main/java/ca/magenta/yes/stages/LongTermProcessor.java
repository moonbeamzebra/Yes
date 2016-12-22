package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.data.LogstashMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;


public class LongTermProcessor implements Runnable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LongTermProcessor.class.getPackage().getName());


    private final String name;

    private BlockingQueue<LogstashMessage> inputQueue;

    private IndexWriter luceneIndexWriter;

    private volatile boolean doRun = true;

    private long olderTimestamp = System.currentTimeMillis();
    private long newerTimestamp = System.currentTimeMillis();
    private final long startTime;
    private long maxQueueLength = 0;
    long previousNow = System.currentTimeMillis();

    private long count = 0;
    private long thisRunCount = 0;
    private long reportCount = 0;


    synchronized public void stopIt() {
        doRun = false;
    }

    private static final long printEvery = 100000;


    public LongTermProcessor(String name, BlockingQueue<LogstashMessage> inputQueue) throws AppException {

        this.name = name;
        this.inputQueue = inputQueue;

        this.startTime = System.currentTimeMillis();

    }

    public void run() {

        logger.info("New LongTermProcessor running");
        doRun = true;
        thisRunCount = 0;
        reportCount = 0;
        olderTimestamp = System.currentTimeMillis();
        newerTimestamp = System.currentTimeMillis();
        try {

            while (doRun || !inputQueue.isEmpty()) {
                LogstashMessage message = inputQueue.take();
                long timestamp = message.getTimestamp();
                if (timestamp < olderTimestamp)
                    olderTimestamp = timestamp;
                if (timestamp > newerTimestamp)
                    newerTimestamp = timestamp;
                logger.debug("LongTermProcessor received: " + message);
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

            try {
                this.luceneIndexWriter.commit();
                this.luceneIndexWriter.close();
            } catch (IOException e1) {
                logger.error("IOException", e);
            }
            if (doRun)
                logger.error("InterruptedException", e);
            else
                logger.info("Processor manager asked to stop!");
        }
    }

    synchronized void printReport()
    {
        long queueLength = inputQueue.size();
        if (queueLength > maxQueueLength)
            maxQueueLength = queueLength;

        long now = System.currentTimeMillis();

        long totalTimeSinceStart = now - startTime;
        float msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;

        long totalTime = now - previousNow;
        float msgPerSec = ((float) reportCount / (float) totalTime) * 1000;

        System.out.println(this.getClass().getSimpleName() + ": " + reportCount +
                " messages sent in " + totalTime +
                " msec; [" + msgPerSec + " msgs/sec] in queue: " + queueLength + "/" + maxQueueLength +
                " trend: [" + msgPerSecSinceStart + " msgs/sec] ");
        previousNow = now;
        reportCount = 0;

    }

    synchronized private void storeInLucene(LogstashMessage message) throws AppException {

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
            document.add(new StringField("LogstasHtimestamp", message.getLogstasHtimestamp(), Field.Store.YES));
            document.add(new StringField("timestamp", Long.toString(message.getTimestamp()), Field.Store.YES));
            document.add(new StringField("device", message.getDevice(), Field.Store.YES));
            document.add(new StringField("source", message.getSource(), Field.Store.YES));
            document.add(new StringField("dest", message.getDest(), Field.Store.YES));
            document.add(new StringField("port", message.getPort(), Field.Store.YES));
            document.add(new StringField("action", message.getAction(), Field.Store.YES));
            document.add(new StringField("customer", message.getCustomer(), Field.Store.YES));
            document.add(new StringField("type", message.getType(), Field.Store.YES));
            document.add(new TextField("message", message.getMessage(), Field.Store.YES));

            luceneIndexWriter.addDocument(document);
            logger.debug("Document added");
            //luceneIndexWriter.commit();
        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }


    }

    public void setInputQueue(BlockingQueue<LogstashMessage> inputQueue)
    {
        this.inputQueue = inputQueue;
    }

    public void createIndex(String indexPath) throws AppException{

        IndexWriter indexWriter = null;

        try {
            Analyzer analyzer = new StandardAnalyzer();
            boolean recreateIndexIfExists = false;

            logger.debug("Indexing in '" + indexPath + "'");


            Directory indexDir = null;
            indexDir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            if (recreateIndexIfExists) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            }

            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(),e);
        }

        this.luceneIndexWriter = indexWriter;
    }

    public long getOlderTimestamp() {
        return olderTimestamp;
    }

    public long getNewerTimestamp() {
        return newerTimestamp;
    }

    public long getThisRunCount() {
        return thisRunCount;
    }

}
