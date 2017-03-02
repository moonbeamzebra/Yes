package ca.magenta.yes.connector.common;

import ca.magenta.utils.Runner;
import ca.magenta.yes.data.NormalizedLogRecord;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class IndexSubscriber extends Runner {


    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final int MAX_QUERY_SIZE = 1000;

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

    public void run() {

        logger.info("New IndexSubscriber " + this.getName() + " running");
        queue = new ArrayBlockingQueue<>(300000);
        IndexReader reader = null;
        try {
            StandardQueryParser queryParserHelper = new StandardQueryParser();
            Query stringQuery = queryParserHelper.parse(searchString, "message");

            // Sorted by ascending index = same order as it comes
            Sort sort = new Sort(new SortField("name", SortField.FIELD_DOC.getType(), false));

            // Sorted by ascending rxTimestamp
            //Sort sort = new Sort(new SortedNumericSortField("rxTimestamp",SortField.Type.LONG, false));

            while (doRun) {
                Directory index = queue.take();

                try {
                    reader = DirectoryReader.open(index);
                    IndexSearcher searcher = new IndexSearcher(reader);

                    TopDocs results = searcher.search(stringQuery, MAX_QUERY_SIZE, sort);
                    if (results.totalHits >= MAX_QUERY_SIZE)
                        logger.warn("Search too wide");
                    if (logger.isDebugEnabled())
                        logger.debug("Number of hits: " + results.totalHits);
                    for (ScoreDoc scoreDoc : results.scoreDocs) {
                        Document doc = searcher.doc(scoreDoc.doc);
                        NormalizedLogRecord normalizedLogRecord = new NormalizedLogRecord(doc);

                        this.forward(normalizedLogRecord.toJson());
                    }
                } catch (IOException e) {
                    logger.error("IOException", e);
                }
            }
        } catch (InterruptedException e) {
            if (doRun)
                logger.error("InterruptedException", e);
        } catch (QueryNodeException e) {
            if (doRun)
                logger.error("QueryNodeException", e);
        } finally {
            try {
                if (reader != null)
                    reader.close();
            } catch (IOException e) {
                logger.error("IOException", e);
            }
        }
        RealTimeProcessorMgmt.indexPublisher().unsubscribe(this);
        queue.clear();
        queue = null;
        logger.debug("IndexSubscriber " + this.getName() + " stops running; queue emptied");
    }

    protected abstract void forward(String message);
}
