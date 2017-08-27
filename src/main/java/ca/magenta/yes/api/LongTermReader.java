package ca.magenta.yes.api;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.NormalizedMsgRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Paths;

public class LongTermReader extends Runner {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LongTermReader.class.getName());

    private static final String ERROR_LABEL = " ERROR: ";
    public static final String END_DATA_STRING = "End of data";

    private final PrintWriter client;

    private final MasterIndex masterIndex;


    private final TimeRange periodTimeRange;
    private final String partition;
    private final int limit;
    private final String searchString;
    private final boolean reverse;



    LongTermReader(String name,
                   MasterIndex masterIndex,
                   String partition,
                   String searchString,
                   Params params,
                   PrintWriter client) {

        super(name);

        this.masterIndex = masterIndex;
        this.partition = partition;
        this.searchString = searchString;
        this.periodTimeRange = params.getTimeRange();
        this.reverse = params.isReverse();
        this.limit = params.getLimit();
        this.client = client;
    }

    @Override
    public void run() {

        logger.info("New LongTermReader " + this.getName() + " running");

        String errorMessage = "";

        try {
            doLongTerm(periodTimeRange, partition, limit, searchString, reverse, client);
        } catch (IOException | QueryNodeException | RuntimeException | ParseException | AppException e) {
            logger.error(e.getClass().getSimpleName(), e);
            errorMessage = ERROR_LABEL + e.getMessage();
        } finally {
            String endOfDataMsg = END_DATA_STRING + errorMessage;
            if (client != null) {
                logger.debug(endOfDataMsg);
                client.println(endOfDataMsg);
            }
        }

        logger.info("IndexSubscriber " + this.getName() + " stops");
    }

    private synchronized void doLongTerm(TimeRange periodTimeRange,
                                          String partition,
                                          int limit,
                                          String searchString,
                                          boolean reverse,
                                          PrintWriter client) throws IOException, QueryNodeException, ParseException, AppException {


        masterIndex.search(this,
                partition,
                periodTimeRange,
                limit,
                searchString,
                reverse,
                client);
    }


    public int searchInLongTermIndex(String longTermIndexName,
                                     TimeRange periodTimeRange,
                                     String searchString,
                                     boolean reverse,
                                     PrintWriter client,
                                     int limit,
                                     int soFarCountParam) throws IOException, QueryNodeException {

        int soFarCount = soFarCountParam;


        String indexNamePath = Globals.getConfig().getLtIndexBaseDirectory() + File.separator + longTermIndexName;

        logger.info("longTermIndexName: {{}}", indexNamePath);
        logger.info("indexNamePath: {{}}", indexNamePath);
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
        IndexSearcher searcher = new IndexSearcher(reader);

        String completeSearchStr = String.format("[%s TO %s] AND %s",
                NormalizedMsgRecord.toStringTimestamp(periodTimeRange.getOlderTime()),
                NormalizedMsgRecord.toStringTimestamp(periodTimeRange.getNewerTime()),
                searchString);

        logger.info("Search[{}]: {{}}", longTermIndexName, completeSearchStr);

        Query stringQuery = NormalizedMsgRecord.buildQuery_messageAsDefaultField(searchString);

        int maxTotalHits = Globals.getConfig().getMaxTotalHit_LongTermIndex();

        Sort sort = NormalizedMsgRecord.buildSort_uidDriving(reverse);

        ObjectMapper mapper = new ObjectMapper();
        ScoreDoc lastScoreDoc = null;
        int totalRead = maxTotalHits; // just to let enter in the following loop
        while ( isDoRun() && (totalRead >= maxTotalHits) && (soFarCount < limit)) {
            TopDocs results;

            results = searcher.searchAfter(lastScoreDoc, stringQuery, maxTotalHits, sort);

            totalRead = results.scoreDocs.length;

            for (ScoreDoc scoreDoc : results.scoreDocs) {
                lastScoreDoc = scoreDoc;

                Document doc = searcher.doc(scoreDoc.doc);
                NormalizedMsgRecord normalizedLogRecord = new NormalizedMsgRecord(doc);

                if (client != null) {
                    if (logger.isDebugEnabled())
                        logger.debug("Out to client");
                    client.println(normalizedLogRecord.toJson(mapper));
                    soFarCount++;
                }

                if ( !isDoRun() || (soFarCount >= limit) )
                {
                    break;
                }
            }
        }

        reader.close();

        return soFarCount;
    }

    public static class Params implements Serializable {

        public Params() {
        }

        public Params(TimeRange timeRange, boolean reverse, int limit) {
            this.timeRange = timeRange;
            this.reverse = reverse;
            this.limit = limit;
        }

        public TimeRange getTimeRange() {
            return timeRange;
        }

        public void setTimeRange(TimeRange timeRange) {
            this.timeRange = timeRange;
        }

        public boolean isReverse() {
            return reverse;
        }

        public void setReverse(boolean reverse) {
            this.reverse = reverse;
        }

        public int getLimit() {
            return limit;
        }

        public void setLimit(int limit) {
            this.limit = limit;
        }

        private TimeRange timeRange = null;
        private boolean reverse = false;
        private int limit = 0;

    }
}
