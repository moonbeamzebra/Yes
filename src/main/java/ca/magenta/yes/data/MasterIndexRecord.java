package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.Globals;
import ca.magenta.yes.Yes;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.*;
import org.slf4j.LoggerFactory;

public class MasterIndexRecord {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MasterIndexRecord.class.getName());

    private static final String OLDER_SOURCE_TIMESTAMP_FIELD_NAME = "olderSrcTimestamp";
    private static final String NEWER_SOURCE_TIMESTAMP_FIELD_NAME = "newerSrcTimestamp";
    private static final String OLDER_RECEIVE_TIMESTAMP_FIELD_NAME = "olderRxTimestamp";
    private static final String NEWER_RECEIVE_TIMESTAMP_FIELD_NAME = "newerRxTimestamp";
    private static final String RUN_START_TIMESTAMP_FIELD_NAME = "runStartTimestamp";
    private static final String RUN_END_TIMESTAMP_FIELD_NAME = "runEndTimestamp";
    private static final String PARTITION_FIELD_NAME = "partition";
    private static final String LONG_TERM_INDEX_NAME_FIELD_NAME = "longTermIndexName";

    private final RuntimeTimestamps runtimeTimestamps;
    private final String longTermIndexName;
    private final String partition;

    public MasterIndexRecord(Document masterIndexDoc) {

        runtimeTimestamps = new RuntimeTimestamps(
                Long.valueOf(masterIndexDoc.get(OLDER_SOURCE_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(NEWER_SOURCE_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(OLDER_RECEIVE_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(NEWER_RECEIVE_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(RUN_START_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(RUN_END_TIMESTAMP_FIELD_NAME))
        );

        this.longTermIndexName = masterIndexDoc.get(LONG_TERM_INDEX_NAME_FIELD_NAME);
        this.partition = masterIndexDoc.get(PARTITION_FIELD_NAME);
    }

    public MasterIndexRecord(String longTermIndexName, String partition, RuntimeTimestamps runtimeTimestamps) {
        this.longTermIndexName = longTermIndexName;
        this.partition = partition;
        this.runtimeTimestamps = runtimeTimestamps;
    }

    Document toDocument() throws AppException {

        Document document = new Document();

        LuceneTools.storeSortedNumericDocValuesField(document, OLDER_SOURCE_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getOlderSrcTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, NEWER_SOURCE_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getNewerSrcTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, OLDER_RECEIVE_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getOlderRxTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, NEWER_RECEIVE_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getNewerRxTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, RUN_START_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getRunStartTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, RUN_END_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getRunEndTimestamp());

        LuceneTools.luceneStoreNonTokenizedString(document, PARTITION_FIELD_NAME, partition);

        document.add(new StringField(LONG_TERM_INDEX_NAME_FIELD_NAME, longTermIndexName, Field.Store.YES));

        return document;
    }


    @Override
    public String toString() {
        return "MasterIndexRecord{" +
                "partition='" + partition +
                "', longTermIndexName='" + longTermIndexName +
                "', olderSrc=" + runtimeTimestamps.getOlderSrcTimestamp() +
                ", newerSrc=" + runtimeTimestamps.getNewerSrcTimestamp() +
                ", olderRx=" + runtimeTimestamps.getOlderRxTimestamp() +
                ", newerRx=" + runtimeTimestamps.getNewerRxTimestamp() +
                ", runStart=" + runtimeTimestamps.getRunStartTimestamp() +
                ", runEnd=" + runtimeTimestamps.getRunEndTimestamp() +
                '}';
    }

    public String getLongTermIndexName() {
        return longTermIndexName;
    }

    public static BooleanQuery buildSearchStringForTimeRangeAndPartition(Globals.DrivingTimestamp drivingTimestamp,
                                                                         String partition, TimeRange periodTimeRange) {

        if (drivingTimestamp == Globals.DrivingTimestamp.SOURCE_TIME)
        {
            return buildSearchStringForTimeRangeAndPartition(partition,
                    periodTimeRange,
                    OLDER_SOURCE_TIMESTAMP_FIELD_NAME,
                    NEWER_SOURCE_TIMESTAMP_FIELD_NAME);
        }
        else
        {
            return buildSearchStringForTimeRangeAndPartition(partition,
                    periodTimeRange,
                    OLDER_RECEIVE_TIMESTAMP_FIELD_NAME,
                    NEWER_RECEIVE_TIMESTAMP_FIELD_NAME);
        }
    }

    private static BooleanQuery buildSearchStringForTimeRangeAndPartition(String partition,
                                                                          TimeRange periodTimeRange,
                                                                          String olderTimestampFieldName,
                                                                          String newerTimestampFieldName) {

        // Files containing range at the end : left part
        // olderRxTimestamp <= OlderTimeRange <= newerRxTimestamp
        BooleanQuery bMasterSearchLeftPart = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery(olderTimestampFieldName, 0, periodTimeRange.getOlderTime()), BooleanClause.Occur.MUST).
                add(LongPoint.newRangeQuery(newerTimestampFieldName, periodTimeRange.getOlderTime(), Long.MAX_VALUE), BooleanClause.Occur.MUST).
                build();


        // Range completely enclose the files range: middle part
        // OlderTimeRange <= olderRxTimestamp AND newerRxTimestamp <= NewerTimeRange
        BooleanQuery bMasterSearchMiddlePart = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery(olderTimestampFieldName, periodTimeRange.getOlderTime(), Long.MAX_VALUE), BooleanClause.Occur.MUST).
                add(LongPoint.newRangeQuery(newerTimestampFieldName, 0, periodTimeRange.getNewerTime()), BooleanClause.Occur.MUST).
                build();


        // Files containing range at the beginning : right part
        // olderRxTimestamp <= NewerTimeRange <= newerRxTimestamp
        BooleanQuery bMasterSearchRightPart = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery(olderTimestampFieldName, 0, periodTimeRange.getNewerTime()), BooleanClause.Occur.MUST).
                add(LongPoint.newRangeQuery(newerTimestampFieldName, periodTimeRange.getNewerTime(), Long.MAX_VALUE), BooleanClause.Occur.MUST).
                build();

        // File range completely enclose the range: narrow part
        // olderRxTimestamp <= OlderTimeRange AND NewerTimeRange <= newerRxTimestamp
        BooleanQuery bMasterSearchNarrowPart = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery(olderTimestampFieldName, 0, periodTimeRange.getOlderTime()), BooleanClause.Occur.MUST).
                add(LongPoint.newRangeQuery(newerTimestampFieldName, periodTimeRange.getNewerTime(), Long.MAX_VALUE), BooleanClause.Occur.MUST).
                build();


        BooleanQuery.Builder returnedBooleanQueryBuilder = new BooleanQuery.Builder();
        if (partition != null) {
            StandardQueryParser queryParserHelper = new StandardQueryParser();
            Query partitionQuery = null;
            try {
                partitionQuery = queryParserHelper.parse(String.format("%s:%s", PARTITION_FIELD_NAME, partition), PARTITION_FIELD_NAME);
                returnedBooleanQueryBuilder.add(partitionQuery, BooleanClause.Occur.SHOULD);
            } catch (QueryNodeException e) {
                logger.error(e.getClass().getSimpleName(),e);
            }
        }

        returnedBooleanQueryBuilder.
                    add(bMasterSearchLeftPart, BooleanClause.Occur.SHOULD).
                    add(bMasterSearchMiddlePart, BooleanClause.Occur.SHOULD).
                    add(bMasterSearchRightPart, BooleanClause.Occur.SHOULD).
                    add(bMasterSearchNarrowPart, BooleanClause.Occur.SHOULD);

        return returnedBooleanQueryBuilder.build();

    }

    public static Sort buildSort_receiveTimeDriving(boolean reverse) {
        return new Sort(new SortedNumericSortField(OLDER_RECEIVE_TIMESTAMP_FIELD_NAME, SortField.Type.LONG, reverse));
    }

    public static class RuntimeTimestamps {

        private long olderSrcTimestamp;
        private long newerSrcTimestamp;

        private long olderRxTimestamp;
        private long newerRxTimestamp;

        private final long runStartTimestamp;
        private long runEndTimestamp;

        public RuntimeTimestamps() {
            runStartTimestamp = System.currentTimeMillis();

            olderSrcTimestamp = Long.MAX_VALUE;
            newerSrcTimestamp = 0;

            olderRxTimestamp = Long.MAX_VALUE;
            newerRxTimestamp = 0;
        }

        private RuntimeTimestamps(long olderSrcTimestamp,
                                  long newerSrcTimestamp,
                                  long olderRxTimestamp,
                                  long newerRxTimestamp,
                                  long runStartTimestamp,
                                  long runEndTimestamp) {
            this.olderSrcTimestamp = olderSrcTimestamp;
            this.newerSrcTimestamp = newerSrcTimestamp;
            this.olderRxTimestamp = olderRxTimestamp;
            this.newerRxTimestamp = newerRxTimestamp;
            this.runStartTimestamp = runStartTimestamp;
            this.runEndTimestamp = runEndTimestamp;
        }

        public void compute(long srcTimestamp, long rxTimestamp) {

            if (srcTimestamp < olderSrcTimestamp)
                olderSrcTimestamp = srcTimestamp;
            if (srcTimestamp > newerSrcTimestamp)
                newerSrcTimestamp = srcTimestamp;

            if (rxTimestamp < olderRxTimestamp)
                olderRxTimestamp = rxTimestamp;
            if (rxTimestamp > newerRxTimestamp)
                newerRxTimestamp = rxTimestamp;
        }

        long getOlderSrcTimestamp() {
            return olderSrcTimestamp;
        }

        long getNewerSrcTimestamp() {
            return newerSrcTimestamp;
        }

        long getOlderRxTimestamp() {
            return olderRxTimestamp;
        }

        public long getNewerRxTimestamp() {
            return newerRxTimestamp;
        }

        public long getRunStartTimestamp() {
            return runStartTimestamp;
        }

        public long getRunEndTimestamp() {
            return runEndTimestamp;
        }

        public void setRunEndTimestamp(long runEndTimestamp) {
            this.runEndTimestamp = runEndTimestamp;
        }
    }


}
