package ca.magenta.yes.api;

import ca.magenta.utils.TimeRange;
import ca.magenta.yes.connector.common.IndexSubscriber;
import ca.magenta.yes.data.MasterIndexRecord;
import ca.magenta.yes.data.NormalizedLogRecord;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author jean-paul.laberge <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-12-07
 */
public class LongTermReader implements Runnable {


    public static Logger logger = Logger.getLogger(LongTermReader.class);

    private final PrintWriter client;

    private final String indexBaseDirectory;

    private final String name;
    private final TimeRange periodTimeRange;
    private final String searchString;

    private volatile boolean doRun = true;

    public void stop() {
        doRun = false;
    }


    public LongTermReader(String name, String indexBaseDirectory, TimeRange periodTimeRange, String searchString, PrintWriter client) {
        this.name = name;
        this.indexBaseDirectory = indexBaseDirectory;
        this.periodTimeRange = periodTimeRange;
        this.searchString = searchString;
        this.client = client;
    }

    public void run() {

        logger.debug("New LongTermReader " + name + " running");

        try {
            doLongTerm(indexBaseDirectory, periodTimeRange, searchString, client);
            if (client != null) {
                logger.debug("End of data");
                client.println("End of data");
            }
        } catch (IOException e) {
            logger.error("IOException", e);
        } catch (ParseException e) {
            logger.error("ParseException", e);
        }
    }

    private static void doLongTerm(String indexBaseDirectory, TimeRange periodTimeRange, String searchString, PrintWriter client) throws IOException, ParseException {

        String masterSearch = "";

        // Files containing range at the end : left part
        // olderRxTimestamp <= OlderTimeRange <= newerRxTimestamp
        String masterSearchLeftPart = String.format("olderRxTimestamp:[%d TO %d] AND newerRxTimestamp:[%d TO %d]",
                0,
                periodTimeRange.getOlderTime(),
                periodTimeRange.getOlderTime(),
                Long.MAX_VALUE);
        //logger.info("masterSearchLeftPart");
        //searchIndex(masterSearchLeftPart);


        // Range completely enclose the files range: middle part
        // OlderTimeRange <= olderRxTimestamp AND newerRxTimestamp <= NewerTimeRange
        String masterSearchMiddlePart = String.format("olderRxTimestamp:[%d TO %d] AND newerRxTimestamp:[%d TO %d]",
                periodTimeRange.getOlderTime(),
                Long.MAX_VALUE,
                0,
                periodTimeRange.getNewerTime());
        //logger.info("masterSearchMiddlePart");
        //searchIndex(masterSearchMiddlePart);


        // Files containing range at the beginning : right part
        // olderRxTimestamp <= NewerTimeRange <= newerRxTimestamp
        String masterSearchRightPart = String.format("olderRxTimestamp:[%d TO %d] AND newerRxTimestamp:[%d TO %d]",
                0,
                periodTimeRange.getNewerTime(),
                periodTimeRange.getNewerTime(),
                Long.MAX_VALUE);
        //logger.info("masterSearchRightPart");
        //searchIndex(masterSearchRightPart);

        // File range completely enclose the range: narrow part
        // olderRxTimestamp <= OlderTimeRange AND NewerTimeRange <= newerRxTimestamp
        String masterSearchNarrowPart = String.format("olderRxTimestamp:[%d TO %d] AND newerRxTimestamp:[%d TO %d]",
                0,
                periodTimeRange.getOlderTime(),
                periodTimeRange.getNewerTime(),
                Long.MAX_VALUE);
        //logger.info("masterSearchNarrowPart");
        //searchIndex(masterSearchNarrowPart);

        masterSearch = String.format("(%s) OR (%s) OR (%s) OR (%s)",
                masterSearchLeftPart,
                masterSearchMiddlePart,
                masterSearchRightPart,
                masterSearchNarrowPart);

        searchMasterIndex(indexBaseDirectory, masterSearch, periodTimeRange, searchString, client);

        return;
    }

    public static void searchMasterIndex(String indexBaseDirectory,
                                         String masterSearchString,
                                         TimeRange periodTimeRange,
                                         String searchString,
                                         PrintWriter client) throws IOException, ParseException, ParseException {

        String indexNamePath = indexBaseDirectory + File.separator + "master.lucene";

        //System.out.println("Searching for '" + masterSearchString + "' in " +  indexNamePath);
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
        IndexSearcher searcher = new IndexSearcher(reader);


        Analyzer analyzer = new KeywordAnalyzer();
        //Analyzer analyzer = new StandardAnalyzer();
        QueryParser queryParser = new QueryParser("message", analyzer);
        Query query = queryParser.parse(masterSearchString);
        TopDocs results = searcher.search(query, 1000);
        //System.out.println("Number of hits: " + results.totalHits);
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            //System.out.println(String.format("scoreDocs:[%s]",scoreDocs.toString()));
            Document doc = searcher.doc(scoreDoc.doc);
            MasterIndexRecord masterIndexRecord = new MasterIndexRecord(doc);
            //String key = String.format("{timestamp : %s, device : %s, source : %s, dest : %s, port : %s }",doc.get("timestamp"),doc.get("device"),doc.get("source"),doc.get("dest"),doc.get("port") );
            //System.out.println("Found:" + doc.toString());
            //System.out.println("Found:" + masterIndexRecord.toString());
            searchLongTermIndex(indexBaseDirectory, masterIndexRecord.getLongTermIndexName(), periodTimeRange, searchString, client);

        }

        reader.close();

        //deleteDirFile(new File(searchIndexDirectory));

    }

    public static void searchLongTermIndex(String indexBaseDirectory,
                                           String longTermIndexName,
                                           TimeRange periodTimeRange,
                                           String searchString,
                                           PrintWriter client) throws IOException, ParseException, ParseException {

        String timeRangeStr = String.format("rxTimestamp:[%d TO %d]",
                periodTimeRange.getOlderTime(),
                periodTimeRange.getNewerTime());

        String completeSearchStr = String.format("(%s) AND (%s)",
                timeRangeStr,
                searchString);

        //completeSearchStr = timeRangeStr;
        //completeSearchStr = searchString;

        String indexNamePath = indexBaseDirectory + File.separator + longTermIndexName;

        //System.out.println("Searching for '" + completeSearchStr + "' in " +  indexNamePath);
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
        IndexSearcher searcher = new IndexSearcher(reader);


        Analyzer analyzer = new KeywordAnalyzer();
        //Analyzer analyzer = new StandardAnalyzer();
        QueryParser queryParser = new QueryParser("message", analyzer);
        Query query = queryParser.parse(completeSearchStr);
        TopDocs results = searcher.search(query, 1000);
        //System.out.println("Number of hits: " + results.totalHits);
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            //System.out.println(String.format("scoreDocs:[%s]",scoreDocs.toString()));
            Document doc = searcher.doc(scoreDoc.doc);
            //String key = String.format("{timestamp : %s, device : %s, source : %s, dest : %s, port : %s }",doc.get("timestamp"),doc.get("device"),doc.get("source"),doc.get("dest"),doc.get("port") );
            //System.out.println("Found:" + doc.toString());
            NormalizedLogRecord normalizedLogRecord = new NormalizedLogRecord(doc);
            //System.out.println("Found:" + normalizedLogRecord.toString());

            if (client != null) {
                logger.debug("Out to client");
                client.println(normalizedLogRecord.toJson());
            }


        }

        reader.close();

        //deleteDirFile(new File(searchIndexDirectory));

    }

}
