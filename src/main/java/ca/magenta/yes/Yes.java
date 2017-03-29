package ca.magenta.yes;

import ca.magenta.yes.client.YesClient;
import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.data.NormalizedMsgRecord;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

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
    private static YesClient.OutputOption outputOption = YesClient.OutputOption.DEFAULT;
    private static TimeRange periodTimeRange = null;

    public static void main(String[] args) throws IOException, ParseException {

        logger.info("");
        logger.info("Running Yes version " + version);

        int rc = parseParam(args);

        if (rc == 0) {

            YesClient yesClient = new YesClient(apiServerAddr, apiServerPort);

            if (realTime) {
                doRealTime();
            } else if (longTerm) {
                yesClient.showLongTermEntries(periodTimeRange, searchString, outputOption);
            }

        }
    }

//    private static void doLongTerm(TimeRange periodTimeRange, String searchString) {
//        try {
//
//            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
//            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);
//
//            String control = String.format("{\"mode\":\"longTerm\",\"olderTime\":\"%s\",\"newerTime\":\"%s\",\"searchString\":\"%s\"}",
//                    periodTimeRange.getOlderTime(),
//                    periodTimeRange.getNewerTime(),
//                    searchString);
//
//            toServer.println(control);
//
//            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));
//
//            String entry;
//            boolean doRun = true;
//
//            String lastMessage = "";
//            while (doRun && (entry = fromServer.readLine()) != null) {
//                if (!(entry.startsWith(LongTermReader.END_DATA_STRING))) {
//                    YesClient.printEntry(entry,outputOption);
//                } else {
//                    lastMessage = entry;
//                    doRun = false;
//                }
//
//            }
//
//            fromServer.close();
//            apiServer.close();
//
//            if (lastMessage.length() > LongTermReader.END_DATA_STRING.length()) {
//                logger.error(String.format("%s", lastMessage));
//            }
//
//        } catch (Throwable t) {
//            t.printStackTrace();
//        }
//    }

//    private static void printEntry(String entry) throws IOException {
//
//        NormalizedMsgRecord normalizedLogRecord = null;
//
//        if (outputOption != OutputOption.JSON)
//            normalizedLogRecord = NormalizedMsgRecord.fromJson(entry);
//
//        // DEFAULT, RAW, JSON, TWO_LINER
//        switch (outputOption) {
//            case JSON:
//                System.out.println(entry);
//                break;
//            case RAW:
//                System.out.println(String.format("[%s][%s] %s",
//                        normalizedLogRecord.getPrettyRxTimestamp(),
//                        normalizedLogRecord.getPartition(),
//                        normalizedLogRecord.getMessage()));
//                break;
//            case TWO_LINER:
//                System.out.println(normalizedLogRecord.toTTYString(true, false));
//                break;
//
//            case DEFAULT:
//                System.out.println(normalizedLogRecord.toTTYString(false, true));
//        }
//    }

    private static void doRealTime() {
        try {

            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

            String control = String.format("{\"mode\":\"realTime\",\"searchString\":\"%s\"}", searchString);

            toServer.println(control);

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

            String entry;

            while ((entry = fromServer.readLine()) != null) {
                YesClient.printEntry(NormalizedMsgRecord.fromJson(entry), outputOption);

            }

            fromServer.close();
            apiServer.close();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


    private static int parseParam(String a_sArgs[]) {
        int rc = 0;

        if (a_sArgs.length > 0) {
            for (String a_sArg : a_sArgs) {
                if (a_sArg.startsWith("-apiServerAddr=")) {
                    apiServerAddr = a_sArg.substring(15);
                    logger.info("apiServerAddr: [" + apiServerAddr + "]");
                } else if (a_sArg.startsWith("-apiServerPort=")) {
                    String apiServerPortStr = a_sArg.substring(15);
                    try {
                        apiServerPort = Integer.parseInt(apiServerPortStr);
                        logger.info("apiServerPort: [" + apiServerPort + "]");

                    } catch (NumberFormatException e) {
                        logger.error("Bad apiServerPort: [" + apiServerPortStr + "]");
                        rc = 1;
                    }
                    // --raw|--json|--2liner
                } else if (a_sArg.toLowerCase().equals("--raw")) {
                    outputOption = YesClient.OutputOption.RAW;
                    logger.info("Output: RAW");
                } else if (a_sArg.toLowerCase().equals("--json")) {
                    outputOption = YesClient.OutputOption.JSON;
                    logger.info("Output: JSON");
                } else if (a_sArg.toLowerCase().equals("--2liner")) {
                    outputOption = YesClient.OutputOption.TWO_LINER;
                    logger.info("Output: 2LINER");
                } else if (a_sArg.toLowerCase().equals("-f")) {
                    realTime = true;
                    logger.info("Real Time Mode");
                } else if (a_sArg.startsWith("--time=")) {
                    logger.info("Long Term Mode");
                    String periodString = a_sArg.substring(7);
                    logger.info("periodString: [" + periodString + "]");
                    try {
                        periodTimeRange = TimeRange.returnTimeRangeBackwardFromNow(periodString);
                        longTerm = true;
                        logger.info(periodTimeRange.toString());
                    } catch (AppException e) {
                        logger.error(String.format("Bad periodString format: [%s]", e.getMessage()));
                        rc = 1;
                    }
                } else {
                    String searchStringPart = a_sArg;
                    if (searchString == null)
                        searchString = searchStringPart;
                    else
                        searchString = searchString + " " + searchStringPart;
                }
            }
        }

        if (((!realTime) && (!longTerm)) ||
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
            if ("*".endsWith(searchString.trim()))
                searchString = "*:*";

            StandardQueryParser queryParserHelper = new StandardQueryParser();
            try {
                queryParserHelper.parse(searchString, "message");
            } catch (QueryNodeException e) {
                System.err.println(String.format("Bad Lucene search string : [%s]", e.getMessage()));
                System.err.println("SEE: https://lucene.apache.org/core/6_2_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package.description");
                rc = 1;
            }
        }

        return rc;
    }

//    private enum OutputOption {
//        DEFAULT, RAW, JSON, TWO_LINER
//    }


}
