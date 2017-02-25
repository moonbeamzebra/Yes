package ca.magenta.yes;

import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.api.LongTermReader;
import ca.magenta.yes.data.NormalizedLogRecord;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * @author moonbeam <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-12-03
 */
public class Yes {

/*
// To run:
java -jar \
-Dloader.main=ca.magenta.yes.Yes \
target/ca.magenta.yes-1.0-SNAPSHOT.jar \
-f -apiServerAddr=127.0.0.1 -apiServerPort=9595 10.10.10.30

java -jar \
-Dloader.main=ca.magenta.yes.Yes \
target/ca.magenta.yes-1.0-SNAPSHOT.jar \
-f -apiServerAddr=10.199.1.25 -apiServerPort=9595 10.10.10.30
*/

    private static String version = "0.1 (2016-12-23)";

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Yes.class.getPackage().getName());

    private static String searchString = null;
    private static boolean realTime = false;
    private static String apiServerAddr = null;
    private static int apiServerPort = -1;
    private static boolean longTerm = false;
    private static OutputOption outputOption = OutputOption.DEFAULT;
    //private static String masterIndexPath = null;
    private static TimeRange periodTimeRange = null;

    public static final void main(String[] args) throws IOException, ParseException {

        int rc = 0;

        logger.info("");
        logger.info("Running Yes version " + version);

        rc = parseParam(args);

        if (rc == 0) {

            if (realTime) {
                doRealTime();
            } else if (longTerm) {
                doLongTerm(periodTimeRange, searchString);
            }

        }
    }

    private static void doLongTerm(TimeRange periodTimeRange, String searchString) {
        try {

            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

            String control = String.format("{\"mode\":\"longTerm\",\"olderTime\":\"%s\",\"newerTime\":\"%s\",\"searchString\":\"%s\"}",
                    periodTimeRange.getOlderTime(),
                    periodTimeRange.getNewerTime(),
                    searchString);

            toServer.println(control);

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

            String entry;
            boolean doRun = true;

            String lastMessage = "";
            while (doRun && (entry = fromServer.readLine()) != null) {
                if (!(entry.startsWith(LongTermReader.END_DATA_STRING))) {
                    printEntry(entry);
                } else {
                    lastMessage = entry;
                    doRun = false;
                }

            }

            fromServer.close();
            apiServer.close();

            if (lastMessage.length() > LongTermReader.END_DATA_STRING.length()) {
                logger.error(String.format("%s", lastMessage));
            }

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private static void printEntry(String entry) throws IOException {

        NormalizedLogRecord normalizedLogRecord = null;

        if (outputOption != outputOption.JSON)
            normalizedLogRecord = NormalizedLogRecord.fromJson(entry);

        // DEFAULT, RAW, JSON, TWO_LINER
        switch (outputOption) {
            case JSON:
                System.out.println(entry);
                break;
            case RAW:
                System.out.println(String.format("[%s][%s] %s",
                        normalizedLogRecord.prettyRxTimestamp(),
                        normalizedLogRecord.getPartition(),
                        normalizedLogRecord.getMessage()));
                break;
            case TWO_LINER:
                System.out.println(normalizedLogRecord.toTTYString(true, false));
                break;

            case DEFAULT:
                System.out.println(normalizedLogRecord.toTTYString(false, true));
        }
    }

    private static void doRealTime() {
        try {

            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

            String control = String.format("{\"mode\":\"realTime\",\"searchString\":\"%s\"}", searchString);

            toServer.println(control);

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

            String entry;

            while ((entry = fromServer.readLine()) != null) {
                printEntry(entry);

            }

            fromServer.close();
            apiServer.close();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

//    private static void doLongTermOld(TimeRange periodTimeRange, String searchString) throws IOException, ParseException {
//
//        String masterSearch = "";
//
//        // Files containing range at the end : left part
//        // olderRxTimestamp <= OlderTimeRange <= newerRxTimestamp
//        String masterSearchLeftPart = String.format("olderRxTimestamp:[%d TO %d] AND newerRxTimestamp:[%d TO %d]",
//                0,
//                periodTimeRange.getOlderTime(),
//                periodTimeRange.getOlderTime(),
//                Long.MAX_VALUE);
//        //logger.info("masterSearchLeftPart");
//        //searchIndex(masterSearchLeftPart);
//
//
//        // Range completely enclose the files range: middle part
//        // OlderTimeRange <= olderRxTimestamp AND newerRxTimestamp <= NewerTimeRange
//        String masterSearchMiddlePart = String.format("olderRxTimestamp:[%d TO %d] AND newerRxTimestamp:[%d TO %d]",
//                periodTimeRange.getOlderTime(),
//                Long.MAX_VALUE,
//                0,
//                periodTimeRange.getNewerTime());
//        //logger.info("masterSearchMiddlePart");
//        //searchIndex(masterSearchMiddlePart);
//
//
//        // Files containing range at the beginning : right part
//        // olderRxTimestamp <= NewerTimeRange <= newerRxTimestamp
//        String masterSearchRightPart = String.format("olderRxTimestamp:[%d TO %d] AND newerRxTimestamp:[%d TO %d]",
//                0,
//                periodTimeRange.getNewerTime(),
//                periodTimeRange.getNewerTime(),
//                Long.MAX_VALUE);
//        //logger.info("masterSearchRightPart");
//        //searchIndex(masterSearchRightPart);
//
//        // File range completely enclose the range: narrow part
//        // olderRxTimestamp <= OlderTimeRange AND NewerTimeRange <= newerRxTimestamp
//        String masterSearchNarrowPart = String.format("olderRxTimestamp:[%d TO %d] AND newerRxTimestamp:[%d TO %d]",
//                0,
//                periodTimeRange.getOlderTime(),
//                periodTimeRange.getNewerTime(),
//                Long.MAX_VALUE);
//        //logger.info("masterSearchNarrowPart");
//        //searchIndex(masterSearchNarrowPart);
//
//        masterSearch = String.format("(%s) OR (%s) OR (%s) OR (%s)",
//                masterSearchLeftPart,
//                masterSearchMiddlePart,
//                masterSearchRightPart,
//                masterSearchNarrowPart);
//
//        searchMasterIndex(masterSearch, periodTimeRange, searchString);
//
//        return;
//    }

//    public static void searchMasterIndex(String masterSearchString, TimeRange periodTimeRange, String searchString) throws IOException, ParseException, ParseException {
//
//        String indexNamePath = masterIndexPath + File.separator + "master.lucene";
//
//        System.out.println("Searching for '" + masterSearchString + "' in " +  indexNamePath);
//        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
//        IndexSearcher searcher = new IndexSearcher(reader);
//
//
//        Analyzer analyzer = new KeywordAnalyzer();
//        //Analyzer analyzer = new StandardAnalyzer();
//        QueryParser queryParser = new QueryParser("message", analyzer);
//        Query query = queryParser.parse(masterSearchString);
//        TopDocs results = searcher.search(query, 1000);
//        System.out.println("Number of hits: " + results.totalHits);
//        for (ScoreDoc scoreDoc : results.scoreDocs) {
//            //System.out.println(String.format("scoreDocs:[%s]",scoreDocs.toString()));
//            Document doc = searcher.doc(scoreDoc.doc);
//            MasterIndexRecord masterIndexRecord = new MasterIndexRecord(doc);
//            //String key = String.format("{timestamp : %s, device : %s, source : %s, dest : %s, port : %s }",doc.get("timestamp"),doc.get("device"),doc.get("source"),doc.get("dest"),doc.get("port") );
//            //System.out.println("Found:" + doc.toString());
//            System.out.println("Found:" + masterIndexRecord.toString());
//            searchLongTermIndex(masterIndexRecord.getLongTermIndexName(), periodTimeRange, searchString);
//
//        }
//
//        reader.close();
//
//        //deleteDirFile(new File(searchIndexDirectory));
//
//    }

//    public static void searchLongTermIndex(String longTermIndexName, TimeRange periodTimeRange, String searchString) throws IOException, ParseException, ParseException {
//
//        String timeRangeStr = String.format("rxTimestamp:[%d TO %d]",
//                periodTimeRange.getOlderTime(),
//                periodTimeRange.getNewerTime());
//
//        String completeSearchStr = String.format("(%s) AND (%s)",
//                timeRangeStr,
//                searchString);
//
//        //completeSearchStr = timeRangeStr;
//        //completeSearchStr = searchString;
//
//        String indexNamePath = masterIndexPath + File.separator + longTermIndexName;
//
//        System.out.println("Searching for '" + completeSearchStr + "' in " +  indexNamePath);
//        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
//        IndexSearcher searcher = new IndexSearcher(reader);
//
//
//        Analyzer analyzer = new KeywordAnalyzer();
//        //Analyzer analyzer = new StandardAnalyzer();
//        QueryParser queryParser = new QueryParser("message", analyzer);
//        Query query = queryParser.parse(completeSearchStr);
//        TopDocs results = searcher.search(query, 1000);
//        System.out.println("Number of hits: " + results.totalHits);
//        for (ScoreDoc scoreDoc : results.scoreDocs) {
//            //System.out.println(String.format("scoreDocs:[%s]",scoreDocs.toString()));
//            Document doc = searcher.doc(scoreDoc.doc);
//            //String key = String.format("{timestamp : %s, device : %s, source : %s, dest : %s, port : %s }",doc.get("timestamp"),doc.get("device"),doc.get("source"),doc.get("dest"),doc.get("port") );
//            //System.out.println("Found:" + doc.toString());
//            NormalizedLogRecord normalizedLogRecord = new NormalizedLogRecord(doc);
//            System.out.println("Found:" + normalizedLogRecord.toString());
//
//
//        }
//
//        reader.close();
//
//        //deleteDirFile(new File(searchIndexDirectory));
//
//    }


    private static int parseParam(String a_sArgs[]) {
        int rc = 0;

        if (a_sArgs.length > 0) {
            for (int i = 0; i < a_sArgs.length; i++) {
                if (a_sArgs[i].startsWith("-apiServerAddr=")) {
                    apiServerAddr = a_sArgs[i].substring(15);
                    logger.info("apiServerAddr: [" + apiServerAddr + "]");
                } else if (a_sArgs[i].startsWith("-apiServerPort=")) {
                    String apiServerPortStr = a_sArgs[i].substring(15);
                    try {
                        apiServerPort = Integer.parseInt(apiServerPortStr);
                        logger.info("apiServerPort: [" + apiServerPort + "]");

                    } catch (NumberFormatException e) {
                        logger.error("Bad apiServerPort: [" + apiServerPortStr + "]");
                        rc = 1;
                    }
                    // --raw|--json|--2liner
                } else if (a_sArgs[i].toLowerCase().equals("--raw")) {
                    outputOption = OutputOption.RAW;
                    logger.info("Output: RAW");
                } else if (a_sArgs[i].toLowerCase().equals("--json")) {
                    outputOption = OutputOption.JSON;
                    logger.info("Output: JSON");
                } else if (a_sArgs[i].toLowerCase().equals("--2liner")) {
                    outputOption = OutputOption.TWO_LINER;
                    logger.info("Output: 2LINER");
                } else if (a_sArgs[i].toLowerCase().equals("-f")) {
                    realTime = true;
                    logger.info("Real Time Mode");
                } else if (a_sArgs[i].startsWith("--time=")) {
                    logger.info("Long Term Mode");
                    String periodString = a_sArgs[i].substring(7);
                    logger.info("periodString: [" + periodString + "]");
                    try {
                        periodTimeRange = TimeRange.returnTimeRangeBackwardFromNow(periodString);
                        longTerm = true;
                        logger.info(periodTimeRange.toString());
                    } catch (AppException e) {
                        logger.error(String.format("Bad periodString format: [%s]", e.getMessage()));
                        rc = 1;
                    }
                }
//                else if (a_sArgs[i].startsWith("-")) {
//                    rc = 1;
//                }
                else {
                    String searchStringPart = a_sArgs[i];
                    if (searchString == null)
                        searchString = searchStringPart;
                    else
                        searchString = searchString + " " + searchStringPart;
                }
            }
        }

        if (((realTime != true) && (longTerm != true)) ||
                (realTime == longTerm)
                ) {
            System.err.println("Select Real Time OR Long Term mode");
            rc = 1;
        } else {
            if (realTime) {
                if ((apiServerAddr == null) ||
                        (apiServerPort == -1) ||
                        (searchString == null)) {
                    System.err.println("In Real Time mode; apiServerAddr, apiServerPort and searchString most be specified");
                    rc = 1;
                }
            } else if (longTerm) {
                if ((apiServerAddr == null) ||
                        (apiServerPort == -1) ||
                        (periodTimeRange == null) ||
                        (searchString == null)) {
                    System.err.println("In Long Term mode; apiServerAddr, apiServerPort, periodString and searchString most be specified");
                    rc = 1;
                }
            }
        }


        if (rc != 0) {
            System.err.println("Usage:");
            System.err.println("  Real Time:");
            System.err.println("    Yes [--raw|--json|--2liner] -f -apiServerAddr=apiServerAddr -apiServerPort=msgServerPort searchString");

            System.err.println("  Long Term:");
            System.err.println("    Yes  [--raw|--json|--2liner] --time=periodString -apiServerAddr=apiServerAddr -apiServerPort=msgServerPort searchString");
            System.err.println("      periodString:");
            System.err.println("        FROMepoch1-TOepoch2");
            System.err.println("        FROMyyyy-MM-ddTHH:mm:ss.SSS-TOyyyy-MM-ddTHH:mm:ss.SSS");
            System.err.println("        LAST2d");
            System.err.println("          where:");
            System.err.println("            y: YEAR (365 days)");
            System.err.println("            M: MONTH (30 days)");
            System.err.println("            w: WEEK");
            System.err.println("            d: DAY");
            System.err.println("            H: HOUR");
            System.err.println("            m: MINUTE");
            System.err.println("            s: SECOND");
            System.err.println("            S: MILLISECOND");

            System.err.println("Ex:");
            System.err.println("  Real Time:");
            System.err.println("    Yes -f -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("  Long Term:");
            System.err.println("    # LAST 3600 SECONDES");
            System.err.println("    Yes --time=LAST3600s -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("    # LAST 15 MINUTES");
            System.err.println("    Yes --time=LAST15m -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("    # LAST 2 HOURS");
            System.err.println("    Yes --time=LAST2H -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("    # LAST 3 DAYS");
            System.err.println("    Yes --time=LAST3d -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("    # LAST 5 WEEKS");
            System.err.println("    Yes --time=LAST5w -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("    # LAST 4 MONTH");
            System.err.println("    Yes --time=LAST4M -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("    # LAST 1 YEAR");
            System.err.println("    Yes --time=LAST1y -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("    # Time range using epoch date");
            System.err.println("    Yes --time=FROM1483104805352-TO1483105405352 -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");
            System.err.println("    # Time range using date string (local time)");
            System.err.println("    Yes --time=FROM2016-12-30T08:33:25.352-TO2016-12-30T08:43:25.352 -apiServerAddr=127.0.0.1 -apiServerPort=9595 'error'");

            rc = 1;
        } else {
            logger.info("searchString: [" + searchString + "]");
            if ( "*".endsWith(searchString.trim()))
                searchString = "*:*";

            StandardQueryParser queryParserHelper = new StandardQueryParser();
            try {
                Query stringQuery = queryParserHelper.parse(searchString, "message");
            } catch (QueryNodeException e) {
                System.err.println(String.format("Bad Lucene search string : [%s]", e.getMessage()));
                System.err.println(String.format(
                "SEE: https://lucene.apache.org/core/6_2_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package.description"));
                rc = 1;
            }
        }

        return rc;
    }

    private enum OutputOption {
        DEFAULT, RAW, JSON, TWO_LINER
    }


}
