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

    private final IndexWriter luceneIndexWriter;

    private volatile boolean doRun = true;


    private long olderTimestamp = System.currentTimeMillis();
    private long newerTimestamp = System.currentTimeMillis();
    private long count = 0;

    public void stopIt() {
        doRun = false;
    }

    private static final long printEvery = 100000;


    public LongTermProcessor(String name, BlockingQueue<LogstashMessage> inputQueue, String indexPath) throws AppException {

        this.name = name;
        this.inputQueue = inputQueue;

        this.luceneIndexWriter = createIndex(indexPath);

    }

    public void run() {

        logger.info("New LongTermProcessor running");
        count = 0;
        long startTime = System.currentTimeMillis();
        long previousNow = startTime;
        long now;
        long totalTime;
        long totalTimeSinceStart;
        float msgPerSec;
        float msgPerSecSinceStart;
        long queueLength = 0;
        long maxQueueLength = 0;
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

                } catch (AppException e) {
                    logger.error("AppException", e);
                }

                queueLength = inputQueue.size();
                if (queueLength > maxQueueLength)
                    maxQueueLength = queueLength;

                if ((count % printEvery) == 0) {
                    now = System.currentTimeMillis();
                    totalTime = now - previousNow;

                    totalTimeSinceStart = now - startTime;
                    msgPerSecSinceStart = ((float) count / (float) totalTimeSinceStart) * 1000;

                    msgPerSec = ((float) printEvery / (float) totalTime) * 1000;

                    System.out.println("LongTermProcessor: " + printEvery +
                            " messages sent in " + totalTime +
                            " msec; [" + msgPerSec + " msgs/sec] in queue: " + queueLength + "/" + maxQueueLength +
                            " trend: [" + msgPerSecSinceStart + " msgs/sec] ");
                    previousNow = now;
                }
            }
        } catch (InterruptedException e) {

            try {
                this.luceneIndexWriter.commit();
                this.luceneIndexWriter.close();
            } catch (IOException e1) {
                logger.error("IOException", e);
            }
            logger.error("InterruptedException", e);
        }
    }

    private void storeInLucene(LogstashMessage message) throws AppException {

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

    public IndexWriter createIndex(String indexPath) throws AppException{

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

        return indexWriter;
    }

    public long getOlderTimestamp() {
        return olderTimestamp;
    }

    public long getNewerTimestamp() {
        return newerTimestamp;
    }

    public long getCount() {
        return count;
    }

}
