package ca.magenta.yes.connector.common;

import ca.magenta.utils.Runner;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class IndexSubscriber extends Runner {


    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static final int MAX_QUERY_SIZE = 1000;

    private final String searchString;

    private BlockingQueue<Directory> queue = null;

    public IndexSubscriber(String name, String searchString) {

        super(name);

        this.searchString = searchString;
        RealTimeProcessorMgmt.indexPublisher().subscribe(this);
    }

    public void store(Directory indexNamePath) throws InterruptedException {
        if (logger.isDebugEnabled())
            logger.debug(String.format("IndexSubscriber receive [%s]", indexNamePath));
        queue.put(indexNamePath);
    }

    @Override
    public void run() {

        logger.info("New IndexSubscriber " + this.getName() + " running");
        queue = new ArrayBlockingQueue<>(300000);
        try {
            Query stringQuery = NormalizedMsgRecord.buildQuery_messageAsDefaultField(searchString);

            // Sorted by ascending index = same order as it comes
            Sort sort = new Sort(new SortField("name", SortField.FIELD_DOC.getType(), false));

            ObjectMapper mapper = new ObjectMapper();
            runLoop(stringQuery, sort, mapper);
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
            Thread.currentThread().interrupt();

        } catch (QueryNodeException e) {
            if (isDoRun())
                logger.error("QueryNodeException", e);
        }
        RealTimeProcessorMgmt.indexPublisher().unsubscribe(this);
        queue.clear();
        queue = null;
        logger.info("IndexSubscriber [{}] stops running; queue emptied", this.getName());
    }

    private void runLoop(Query stringQuery, Sort sort, ObjectMapper mapper) throws InterruptedException {
        while (isDoRun()) {
            Directory index = queue.take();

            try {
                IndexReader reader = DirectoryReader.open(index);
                IndexSearcher searcher = new IndexSearcher(reader);

                TopDocs results = searcher.search(stringQuery, MAX_QUERY_SIZE, sort);
                if (results.totalHits >= MAX_QUERY_SIZE) {
                    logger.warn("Search too wide");
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Number of hits: " + results.totalHits);
                }
                for (ScoreDoc scoreDoc : results.scoreDocs) {
                    Document doc = searcher.doc(scoreDoc.doc);
                    NormalizedMsgRecord normalizedLogRecord = new NormalizedMsgRecord(doc);

                    this.forward(normalizedLogRecord.toJson(mapper));
                }
            } catch (IOException e) {
                logger.error("IOException", e);
            }
        }

    }

    protected abstract void forward(String message);
}
