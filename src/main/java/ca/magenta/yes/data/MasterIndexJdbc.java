package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.Globals;
import ca.magenta.yes.api.LongTermReader;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


// mvn install:install-file -Dfile=src/main/resources/postgresql-42.1.1.jar -DgroupId=postgresql -DartifactId=postgresql -Dversion=42.1.1 -Dpackaging=jar

// https://www.cockroachlabs.com/docs/start-a-local-cluster-in-docker.html#os-linux
// https://www.cockroachlabs.com/docs/build-a-java-app-with-cockroachdb.html
// docker exec -it roach1 ./cockroach user set yesuser --insecure
// docker exec -it roach1 ./cockroach sql --insecure -e 'CREATE DATABASE yes'
// docker exec -it roach1 ./cockroach sql --insecure -e 'GRANT ALL ON DATABASE yes TO yesuser'
// docker exec -it roach1 ./cockroach sql --insecure -e  'ALTER USER yesuser WITH  PASSWORD yespw'

//static final String OLDER_SOURCE_TIMESTAMP_FIELD_NAME = "olderSrcTimestamp";
//static final String NEWER_SOURCE_TIMESTAMP_FIELD_NAME = "newerSrcTimestamp";
//static final String OLDER_RECEIVE_TIMESTAMP_FIELD_NAME = "olderRxTimestamp";
//static final String NEWER_RECEIVE_TIMESTAMP_FIELD_NAME = "newerRxTimestamp";
//static final String RUN_START_TIMESTAMP_FIELD_NAME = "runStartTimestamp";
//static final String RUN_END_TIMESTAMP_FIELD_NAME = "runEndTimestamp";
//static final String PARTITION_FIELD_NAME = "partition";
//static final String LONG_TERM_INDEX_NAME_FIELD_NAME = "longTermIndexName";


public class MasterIndexJdbc extends MasterIndex {

    private static final Logger logger = LoggerFactory.getLogger(MasterIndexJdbc.class.getSimpleName());

    static final String DRIVER_NAME = "org.postgresql.Driver";
    static final String MASTER_INDEX_DB_TABLE = "yesMasterIndex";
    static final String PRIMARY_KEY_FIELD_NAME = "id";
    static final String LIFECYCLE_FIELD_NAME = "lifecycle";
    static final String LIFECYCLE_NEW = "committed";

    private final Pattern urlPattern = Pattern.compile("(jdbc:postgresql:\\/\\/)([^:]+):([^@]*)@(.*)");

    private final String masterIndexEndpoint;
    private final String dbuser;
    private final String dbpw;

    public static MasterIndex getInstance(String masterIndexEndpoint) throws AppException {
        if (instance == null) {
            instance = new MasterIndexJdbc(masterIndexEndpoint);
        }
        return instance;
    }

    private MasterIndexJdbc(String masterIndexEndpoint) throws AppException {
        super();

        // Receive:
        // jdbc:postgresql://yesuser:yespw@127.0.0.1:26257/yes?sslmode=disable

        String dburl = masterIndexEndpoint;
        String dbuser = "";
        String dbpw = "";

        //System.out.println("masterIndexEndpoint: " + masterIndexEndpoint);
        Matcher matcher = urlPattern.matcher(masterIndexEndpoint);
        if (matcher.find()) {
            dburl =  matcher.group(1) + matcher.group(4);
            dbuser =  matcher.group(2);
            dbpw =  matcher.group(3);
            //System.out.println("group 1: " + matcher.group(1));
            //System.out.println("group 2: " + matcher.group(2));
            //System.out.println("group 3: " + matcher.group(3));
            //System.out.println("group 4: " + matcher.group(4));
        }
        this.masterIndexEndpoint = dburl;
        this.dbuser = dbuser;
        this.dbpw = dbpw;
        logger.info(String.format("masterIndexEndpoint : [%s]", this.masterIndexEndpoint));
        logger.info(String.format("dbuser : [%s]", this.dbuser));
        logger.info(String.format("dbpw : [%s]", this.dbpw));
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            throw new AppException(e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void addRecord(MasterIndexRecord masterIndexRecord) throws AppException {

        String insertStr = String.format(
                INSERT_STR,
                masterIndexRecord.getRuntimeTimestamps().getOlderSrcTimestamp(),
                masterIndexRecord.getRuntimeTimestamps().getNewerSrcTimestamp(),
                masterIndexRecord.getRuntimeTimestamps().getOlderRxTimestamp(),
                masterIndexRecord.getRuntimeTimestamps().getNewerRxTimestamp(),
                masterIndexRecord.getRuntimeTimestamps().getRunStartTimestamp(),
                masterIndexRecord.getRuntimeTimestamps().getRunEndTimestamp(),
                masterIndexRecord.getPartitionName(),
                masterIndexRecord.getLongTermIndexName(),
                LIFECYCLE_NEW);

        Connection db = null;
        try {
            db = DriverManager.getConnection(masterIndexEndpoint, dbuser, dbpw);
            try {
                db.createStatement().execute(CREATE_TABLE_STR);

                db.createStatement().execute(insertStr);

            } finally {
                db.close();
            }
        } catch (SQLException e) {
            throw new AppException(e);
        }
    }

    @Override
    public void search(LongTermReader longTermReader,
                       String partition,
                       TimeRange periodTimeRange,
                       int limit,
                       String searchString,
                       boolean reverse,
                       PrintWriter client) throws IOException, ParseException, QueryNodeException, AppException {

        String masterQuery = buildSearchStringForTimeRangeAndPartition(Globals.DrivingTimestamp.RECEIVE_TIME,
                partition,
                periodTimeRange,
                reverse);

        logger.info("TimeRange SQL query In MasterIndex: " + masterQuery);

        // TODO Change the following
        if (limit <= 0) limit = Integer.MAX_VALUE;

        Connection db = null;
        try {
            db = DriverManager.getConnection(masterIndexEndpoint, dbuser, dbpw);
            try {

                ResultSet rs = db.createStatement().executeQuery(masterQuery);

                int soFarCount = 0;
                while (longTermReader.isDoRun() && rs.next() && (soFarCount < limit)) {
                    MasterIndexRecord masterIndexRecord = MasterIndexJdbc.valueOf(rs);
                    //System.out.println(masterIndexRecord.toString());
                    soFarCount = longTermReader.searchInLongTermIndex(masterIndexRecord.getLongTermIndexName(),
                            periodTimeRange,
                            searchString,
                            reverse,
                            client,
                            limit,
                            soFarCount);
                }

            } finally {
                db.close();
            }
        } catch (SQLException e) {
            throw new AppException(e);
        }


//        int soFarCount = 0;
//        Sort sort = MasterIndexJdbc.buildSort_receiveTimeDriving(reverse);
//        ScoreDoc lastScoreDoc = null;
//        int totalRead = maxTotalHits; // just to let enter in the following loop
//        if (indexSearcher != null) {
//            while (longTermReader.isDoRun() && (totalRead >= maxTotalHits) && (soFarCount < limit)) {
//                TopDocs results;
//
//                results = indexSearcher.searchAfter(lastScoreDoc, bMasterSearch, maxTotalHits, sort);
//
//                totalRead = results.scoreDocs.length;
//
//                for (ScoreDoc scoreDoc : results.scoreDocs) {
//                    lastScoreDoc = scoreDoc;
//                    Document doc = indexSearcher.doc(scoreDoc.doc);
//                    MasterIndexRecord masterIndexRecord = MasterIndexJdbc.valueOf(doc);
//                    soFarCount = longTermReader.searchInLongTermIndex(indexBaseDirectory,
//                            masterIndexRecord.getLongTermIndexName(),
//                            periodTimeRange,
//                            searchString,
//                            reverse,
//                            client,
//                            limit,
//                            soFarCount);
//                    if ( !longTermReader.isDoRun() || !(soFarCount < limit) )
//                    {
//                        break;
//                    }
//
//                }
//            }
//        }
    }

    private static MasterIndexRecord valueOf(ResultSet rs) throws AppException {
        MasterIndexRecord.RuntimeTimestamps runtimeTimestamps = null;
        try {
            runtimeTimestamps = new MasterIndexRecord.RuntimeTimestamps(
                    rs.getLong(OLDER_SOURCE_TIMESTAMP_FIELD_NAME),
                    rs.getLong(NEWER_SOURCE_TIMESTAMP_FIELD_NAME),
                    rs.getLong(OLDER_RECEIVE_TIMESTAMP_FIELD_NAME),
                    rs.getLong(NEWER_RECEIVE_TIMESTAMP_FIELD_NAME),
                    rs.getLong(RUN_START_TIMESTAMP_FIELD_NAME),
                    rs.getLong(RUN_END_TIMESTAMP_FIELD_NAME)
            );

            return new MasterIndexRecord(rs.getString(LONG_TERM_INDEX_NAME_FIELD_NAME),
                    rs.getString(PARTITION_FIELD_NAME),
                    runtimeTimestamps);
        } catch (SQLException e) {
            throw new AppException(e);
        }

    }

    private static String buildSearchStringForTimeRangeAndPartition(Globals.DrivingTimestamp drivingTimestamp,
                                                                    String partition, TimeRange periodTimeRange,
                                                                    boolean reverse) {

        if (drivingTimestamp == Globals.DrivingTimestamp.SOURCE_TIME) {
            return buildSearchStringForTimeRangeAndPartition(partition,
                    periodTimeRange,
                    OLDER_SOURCE_TIMESTAMP_FIELD_NAME,
                    NEWER_SOURCE_TIMESTAMP_FIELD_NAME,
                    reverse);
        } else {
            return buildSearchStringForTimeRangeAndPartition(partition,
                    periodTimeRange,
                    OLDER_RECEIVE_TIMESTAMP_FIELD_NAME,
                    NEWER_RECEIVE_TIMESTAMP_FIELD_NAME,
                    reverse);
        }
    }

    private static String buildSearchStringForTimeRangeAndPartition(String partition,
                                                                    TimeRange periodTimeRange,
                                                                    String olderTimestampFieldName,
                                                                    String newerTimestampFieldName,
                                                                    boolean reverse) {


        // Files containing range at the end : left part
        String left = String.format("(%s <= %d) AND (%s >= %d) AND (%s <= %d)",
                olderTimestampFieldName, periodTimeRange.getOlderTime(),
                newerTimestampFieldName, periodTimeRange.getOlderTime(),
                newerTimestampFieldName, periodTimeRange.getNewerTime());


        // Range completely enclose the files range: middle part
        String middle = String.format("(%s >= %d) AND (%s <= %d)",
                olderTimestampFieldName, periodTimeRange.getOlderTime(),
                newerTimestampFieldName, periodTimeRange.getNewerTime());


        // Files containing range at the beginning : right part
        String right = String.format("(%s >= %d) AND (%s <= %d) AND (%s >= %d)",
                olderTimestampFieldName, periodTimeRange.getOlderTime(),
                olderTimestampFieldName, periodTimeRange.getNewerTime(),
                newerTimestampFieldName, periodTimeRange.getNewerTime());

        // File range completely enclose the range: narrow part
        String narrow = String.format("(%s <= %d) AND (%s >= %d)",
                olderTimestampFieldName, periodTimeRange.getOlderTime(),
                newerTimestampFieldName, periodTimeRange.getNewerTime());


        String query = String.format("SELECT * FROM %s WHERE ( (%s) OR (%s) OR (%s) OR (%s) )",
                MASTER_INDEX_DB_TABLE,
                left,
                middle,
                right,
                narrow);


        if (partition != null) {
            query = String.format("%s AND (%s = '%s')", query, PARTITION_FIELD_NAME, partition);
        }

        String orderDirection = "";
        if (reverse) {
            orderDirection = "DESC";
        }

        query = String.format("%s ORDER BY %s %s", query, olderTimestampFieldName, orderDirection);

        return query;
    }

//    private static Sort buildSort_receiveTimeDriving(boolean reverse) {
//        return new Sort(new SortedNumericSortField(OLDER_RECEIVE_TIMESTAMP_FIELD_NAME, SortField.Type.LONG, reverse));
//    }
//
//    private static MasterIndexRecord valueOf(Document masterIndexDoc) {
//
//        MasterIndexRecord.RuntimeTimestamps runtimeTimestamps = new MasterIndexRecord.RuntimeTimestamps(
//                Long.valueOf(masterIndexDoc.get(OLDER_SOURCE_TIMESTAMP_FIELD_NAME)),
//                Long.valueOf(masterIndexDoc.get(NEWER_SOURCE_TIMESTAMP_FIELD_NAME)),
//                Long.valueOf(masterIndexDoc.get(OLDER_RECEIVE_TIMESTAMP_FIELD_NAME)),
//                Long.valueOf(masterIndexDoc.get(NEWER_RECEIVE_TIMESTAMP_FIELD_NAME)),
//                Long.valueOf(masterIndexDoc.get(RUN_START_TIMESTAMP_FIELD_NAME)),
//                Long.valueOf(masterIndexDoc.get(RUN_END_TIMESTAMP_FIELD_NAME))
//        );
//
//        return new MasterIndexRecord(masterIndexDoc.get(LONG_TERM_INDEX_NAME_FIELD_NAME),
//                masterIndexDoc.get(PARTITION_FIELD_NAME),
//                runtimeTimestamps);
//    }
//
//    private Document toDocument(MasterIndexRecord masterIndexRecord) throws AppException {
//
//        Document document = new Document();
//
//        LuceneTools.storeSortedNumericDocValuesField(document, OLDER_SOURCE_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getOlderSrcTimestamp());
//        LuceneTools.storeSortedNumericDocValuesField(document, NEWER_SOURCE_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getNewerSrcTimestamp());
//        LuceneTools.storeSortedNumericDocValuesField(document, OLDER_RECEIVE_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getOlderRxTimestamp());
//        LuceneTools.storeSortedNumericDocValuesField(document, NEWER_RECEIVE_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getNewerRxTimestamp());
//        LuceneTools.storeSortedNumericDocValuesField(document, RUN_START_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getRunStartTimestamp());
//        LuceneTools.storeSortedNumericDocValuesField(document, RUN_END_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getRunEndTimestamp());
//
//        LuceneTools.luceneStoreNonTokenizedString(document, PARTITION_FIELD_NAME, masterIndexRecord.getPartitionName());
//
//        document.add(new StringField(LONG_TERM_INDEX_NAME_FIELD_NAME, masterIndexRecord.getLongTermIndexName(), Field.Store.YES));
//
//        return document;
//    }


    private static final String CREATE_TABLE_STR = String.format(
            "CREATE TABLE IF NOT EXISTS %s " +
                    "(%s bigserial PRIMARY KEY, " +
                    "%s bigint, " +
                    "%s bigint, " +
                    "%s bigint, " +
                    "%s bigint, " +
                    "%s bigint, " +
                    "%s bigint, " +
                    "%s varchar(%d), " +
                    "%s varchar(4096), " +
                    "%s varchar(20))",
            MASTER_INDEX_DB_TABLE,
            PRIMARY_KEY_FIELD_NAME,
            OLDER_SOURCE_TIMESTAMP_FIELD_NAME,
            NEWER_SOURCE_TIMESTAMP_FIELD_NAME,
            OLDER_RECEIVE_TIMESTAMP_FIELD_NAME,
            NEWER_RECEIVE_TIMESTAMP_FIELD_NAME,
            RUN_START_TIMESTAMP_FIELD_NAME,
            RUN_END_TIMESTAMP_FIELD_NAME,
            PARTITION_FIELD_NAME,
            MasterIndexRecord.PARTITION_NAME_MAX_LENGTH,
            LONG_TERM_INDEX_NAME_FIELD_NAME,
            LIFECYCLE_FIELD_NAME);

    private static final String INSERT_STR = String.format(
            "INSERT INTO %s (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            MASTER_INDEX_DB_TABLE,
            OLDER_SOURCE_TIMESTAMP_FIELD_NAME,
            NEWER_SOURCE_TIMESTAMP_FIELD_NAME,
            OLDER_RECEIVE_TIMESTAMP_FIELD_NAME,
            NEWER_RECEIVE_TIMESTAMP_FIELD_NAME,
            RUN_START_TIMESTAMP_FIELD_NAME,
            RUN_END_TIMESTAMP_FIELD_NAME,
            PARTITION_FIELD_NAME,
            LONG_TERM_INDEX_NAME_FIELD_NAME,
            LIFECYCLE_FIELD_NAME) +
            " VALUES (%d,%d,%d,%d,%d,%d,'%s','%s','%s')";

}
