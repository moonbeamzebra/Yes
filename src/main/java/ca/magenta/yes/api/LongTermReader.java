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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
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

    private final String indexBaseDirectory;
    private final MasterIndex masterIndex;


    private final TimeRange periodTimeRange;
    private final String searchString;
    private final boolean reverse;



    LongTermReader(String name,
                   String indexBaseDirectory,
                   TimeRange periodTimeRange,
                   String searchString,
                   MasterIndex masterIndex,
                   boolean reverse,
                   PrintWriter client) {

        super(name);

        this.indexBaseDirectory = indexBaseDirectory;
        this.periodTimeRange = periodTimeRange;
        this.searchString = searchString;
        this.masterIndex = masterIndex;
        this.reverse = reverse;
        this.client = client;
    }

    public void run() {

        logger.debug("New LongTermReader " + this.getName() + " running");

        String errorMessage = "";

        try {
            doLongTerm(indexBaseDirectory, periodTimeRange, searchString, reverse, client);
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

    }

    synchronized  private void doLongTerm(String indexBaseDirectory,
                                          TimeRange periodTimeRange,
                                          String searchString,
                                          boolean reverse,
                                          PrintWriter client) throws IOException, QueryNodeException, ParseException, AppException {

        // Files containing range at the end : left part
        // olderRxTimestamp <= OlderTimeRange <= newerRxTimestamp
        BooleanQuery bMasterSearchLeftPart = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery("olderRxTimestamp", 0, periodTimeRange.getOlderTime()), BooleanClause.Occur.MUST).
                add(LongPoint.newRangeQuery("newerRxTimestamp", periodTimeRange.getOlderTime(), Long.MAX_VALUE), BooleanClause.Occur.MUST).
                build();


        // Range completely enclose the files range: middle part
        // OlderTimeRange <= olderRxTimestamp AND newerRxTimestamp <= NewerTimeRange
        BooleanQuery bMasterSearchMiddlePart = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery("olderRxTimestamp", periodTimeRange.getOlderTime(), Long.MAX_VALUE), BooleanClause.Occur.MUST).
                add(LongPoint.newRangeQuery("newerRxTimestamp", 0, periodTimeRange.getNewerTime()), BooleanClause.Occur.MUST).
                build();


        // Files containing range at the beginning : right part
        // olderRxTimestamp <= NewerTimeRange <= newerRxTimestamp
        BooleanQuery bMasterSearchRightPart = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery("olderRxTimestamp", 0, periodTimeRange.getNewerTime()), BooleanClause.Occur.MUST).
                add(LongPoint.newRangeQuery("newerRxTimestamp", periodTimeRange.getNewerTime(), Long.MAX_VALUE), BooleanClause.Occur.MUST).
                build();

        // File range completely enclose the range: narrow part
        // olderRxTimestamp <= OlderTimeRange AND NewerTimeRange <= newerRxTimestamp
        BooleanQuery bMasterSearchNarrowPart = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery("olderRxTimestamp", 0, periodTimeRange.getOlderTime()), BooleanClause.Occur.MUST).
                add(LongPoint.newRangeQuery("newerRxTimestamp", periodTimeRange.getNewerTime(), Long.MAX_VALUE), BooleanClause.Occur.MUST).
                build();

        BooleanQuery bMasterSearch = new BooleanQuery.Builder().
                add(bMasterSearchLeftPart, BooleanClause.Occur.SHOULD).
                add(bMasterSearchMiddlePart, BooleanClause.Occur.SHOULD).
                add(bMasterSearchRightPart, BooleanClause.Occur.SHOULD).
                add(bMasterSearchNarrowPart, BooleanClause.Occur.SHOULD).
                build();

        logger.info("Search In MasterIndex search string: " + bMasterSearch.toString());


        searchInMasterIndex(indexBaseDirectory, bMasterSearch, periodTimeRange, searchString, reverse, client);
    }

    private void searchInMasterIndex(String indexBaseDirectory,
                                     Query indexQuery,
                                     TimeRange periodTimeRange,
                                     String searchString,
                                     boolean reverse,
                                     PrintWriter client) throws IOException, QueryNodeException, ParseException, AppException {

        // https://wiki.apache.org/lucene-java/ImproveSearchingSpeed

        IndexSearcher indexSearcher;
//        String masterIndexPathName = Globals.getConfig().getIndexBaseDirectory() +
//                File.separator +
//                "master.lucene";
//
//        DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(masterIndexPathName)));
//        indexSearcher = new IndexSearcher(reader);
        Searcher searcher = masterIndex.getSearcher();
        indexSearcher = searcher.getIndexSearcher();


        int maxTotalHits = 1000;

        Sort sort = new Sort(new SortedNumericSortField("olderRxTimestamp",SortField.Type.LONG, reverse));
        ScoreDoc lastScoreDoc = null;
        int totalRead = maxTotalHits; // just to let enter in the following loop
        if (indexSearcher != null) {
            while ((totalRead >= maxTotalHits)) {
                TopDocs results;

                results = indexSearcher.searchAfter(lastScoreDoc, indexQuery, maxTotalHits, sort);

                totalRead = results.scoreDocs.length;

                for (ScoreDoc scoreDoc : results.scoreDocs) {
                    lastScoreDoc = scoreDoc;
                    Document doc = indexSearcher.doc(scoreDoc.doc);
                    MasterIndexRecord masterIndexRecord = new MasterIndexRecord(doc);
                    searchInLongTermIndex(indexBaseDirectory,
                            masterIndexRecord.getLongTermIndexName(),
                            periodTimeRange,
                            searchString,
                            reverse,
                            client);

                }
            }
        }
        //reader.close();
    }

    private void searchInLongTermIndex(String indexBaseDirectory,
                                       String longTermIndexName,
                                       TimeRange periodTimeRange,
                                       String searchString,
                                       boolean reverse,
                                       PrintWriter client) throws IOException, QueryNodeException, ParseException {


        String indexNamePath = indexBaseDirectory + File.separator + longTermIndexName;

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
        IndexSearcher searcher = new IndexSearcher(reader);

        String completeSearchStr = String.format("[%s TO %s] AND %s",
                NormalizedMsgRecord.toStringTimestamp(periodTimeRange.getOlderTime()),
                NormalizedMsgRecord.toStringTimestamp(periodTimeRange.getNewerTime()),
                searchString);

        logger.info("completeSearchStr: " + completeSearchStr);

        StandardQueryParser queryParserHelper = new StandardQueryParser();
        Query stringQuery = queryParserHelper.parse(searchString, NormalizedMsgRecord.MESSAGE_FIELD_NAME);

        int maxTotalHits = 1000;

        //Sort sort = new Sort(new SortedNumericSortField(NormalizedMsgRecord.RECEIVE_TIMESTAMP_FIELD_NAME,SortField.Type.LONG, false));
        Sort sort = new Sort(new SortField(NormalizedMsgRecord.UID_FIELD_NAME, SortField.Type.STRING,reverse));

        ObjectMapper mapper = new ObjectMapper();
        ScoreDoc lastScoreDoc = null;
        int totalRead = maxTotalHits; // just to let enter in the following loop
        while ( (totalRead >= maxTotalHits) ) {
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
                }
            }
        }

        reader.close();
    }

}
