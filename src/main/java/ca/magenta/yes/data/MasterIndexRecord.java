package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

public class MasterIndexRecord {

    private static final String OLDER_SOURCE_TIMESTAMP_FIELD_NAME = "olderTxTimestamp";
    private static final String NEWER_SOURCE_TIMESTAMP_FIELD_NAME = "newerTxTimestamp";
    private static final String OLDER_RECEIVE_TIMESTAMP_FIELD_NAME = "olderRxTimestamp";
    private static final String NEWER_RECEIVE_TIMESTAMP_FIELD_NAME = "newerRxTimestamp";
    private static final String RUN_START_TIMESTAMP_FIELD_NAME = "runStartTimestamp";
    private static final String RUN_END_TIMESTAMP_FIELD_NAME = "runEndTimestamp";
    private static final String LONG_TERM_INDEX_NAME_FIELD_NAME = "longTermIndexName";

    private final RuntimeTimestamps runtimeTimestamps;
    private final String longTermIndexName;

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
    }

    public MasterIndexRecord(String longTermIndexName, RuntimeTimestamps runtimeTimestamps) {
        this.runtimeTimestamps = runtimeTimestamps;
        this.longTermIndexName = longTermIndexName;
    }

    Document toDocument() throws AppException {

        Document document = new Document();

        LuceneTools.storeSortedNumericDocValuesField(document, OLDER_SOURCE_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getOlderSrcTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, NEWER_SOURCE_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getNewerSrcTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, OLDER_RECEIVE_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getOlderRxTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, NEWER_RECEIVE_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getNewerRxTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, RUN_START_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getRunStartTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, RUN_END_TIMESTAMP_FIELD_NAME, runtimeTimestamps.getRunEndTimestamp());

        document.add(new StringField(LONG_TERM_INDEX_NAME_FIELD_NAME, longTermIndexName, Field.Store.YES));

        return document;
    }


    @Override
    public String toString() {
        return "MasterIndexRecord{" +
                "longTermIndexName='" + longTermIndexName +
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
