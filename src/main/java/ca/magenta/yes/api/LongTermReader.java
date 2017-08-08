package ca.magenta.yes.api;

import ca.magenta.utils.AppException;
import ca.magenta.utils.Runner;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import ca.magenta.yes.data.MasterIndexRecord;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.data.Searcher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;

public class LongTermReader extends Runner {

    public static Logger logger = Logger.getLogger(LongTermReader.class);

    public static final String END_DATA_STRING = "End of data";

    private final PrintWriter client;

    //private final String indexBaseDirectory;
    private final MasterIndex masterIndex;


    private final TimeRange periodTimeRange;
    private final String partition;
    private final int limit;
    private final String searchString;
    private final boolean reverse;



    LongTermReader(String name,
                   TimeRange periodTimeRange,
                   String partition,
                   int limit,
                   String searchString,
                   MasterIndex masterIndex,
                   boolean reverse,
                   PrintWriter client) {

        super(name);

        //this.indexBaseDirectory = indexBaseDirectory;
        this.periodTimeRange = periodTimeRange;
        this.partition = partition;
        this.limit = limit;
        this.searchString = searchString;
        this.masterIndex = masterIndex;
        this.reverse = reverse;
        this.client = client;
    }

    public void run() {

        logger.info("New LongTermReader " + this.getName() + " running");

        String errorMessage = "";

        try {
            doLongTerm(periodTimeRange, partition, limit, searchString, reverse, client);
        } catch (IOException e) {
            logger.error("IOException", e);
            errorMessage = " ERROR: " + e.getMessage();
        } catch (QueryNodeException e) {
            logger.error("QueryNodeException", e);
            errorMessage = " ERROR: " + e.getMessage();
        } catch (Throwable e) {
            logger.error(e.getClass().getSimpleName(), e);
            errorMessage = " ERROR: " + e.getMessage();
        }
        finally {
            String endOfDataMsg = END_DATA_STRING + errorMessage;
            if (client != null) {
                logger.debug(endOfDataMsg);
                client.println(endOfDataMsg);
            }
        }

        logger.info("IndexSubscriber " + this.getName() + " stops");
    }

    synchronized  private void doLongTerm(TimeRange periodTimeRange,
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
                                     int soFarCount) throws IOException, QueryNodeException, ParseException {


        String indexNamePath = Globals.getConfig().getLtIndexBaseDirectory() + File.separator + longTermIndexName;

        logger.info(String.format("longTermIndexName: {%s}", indexNamePath));
        logger.info(String.format("indexNamePath: {%s}", indexNamePath));
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
        IndexSearcher searcher = new IndexSearcher(reader);

        String completeSearchStr = String.format("[%s TO %s] AND %s",
                NormalizedMsgRecord.toStringTimestamp(periodTimeRange.getOlderTime()),
                NormalizedMsgRecord.toStringTimestamp(periodTimeRange.getNewerTime()),
                searchString);

        logger.info(String.format("Search[%s]: {%s}", longTermIndexName, completeSearchStr));

        //StandardQueryParser queryParserHelper = new StandardQueryParser();
        //Query stringQuery = queryParserHelper.parse(searchString, NormalizedMsgRecord.MESSAGE_FIELD_NAME);

        //StandardQueryParser queryParserHelper = new StandardQueryParser();
        Query stringQuery = NormalizedMsgRecord.buildQuery_messageAsDefaultField(searchString);
        //Query stringQuery = queryParserHelper.parse(searchString, NormalizedMsgRecord.MESSAGE_FIELD_NAME);

        int maxTotalHits = Globals.getConfig().getMaxTotalHit_LongTermIndex();

        Sort sort = NormalizedMsgRecord.buildSort_uidDriving(reverse);
        //Sort sort = new Sort(new SortField(NormalizedMsgRecord.UID_FIELD_NAME, SortField.Type.STRING,reverse));

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

                if ( !isDoRun() || !(soFarCount < limit) )
                {
                    break;
                }
            }
        }

        reader.close();

        return soFarCount;
    }
}
