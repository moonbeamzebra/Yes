package ca.magenta.yes.data;

import org.apache.lucene.document.Document;

/**
 * Created by jplaberge on 2016-12-31.
 */
public class MasterIndexRecord {

    private final long olderTxTimestamp;
    private final long newerTxTimestamp;
    private final long olderRxTimestamp;
    private final long newerRxTimestamp;
    private final long runStartTimestamp;
    private final long runEndTimestamp;
    private final String longTermIndexName;

    public MasterIndexRecord(Document masterIndexDoc) {
        this.olderTxTimestamp = Long.valueOf(masterIndexDoc.get("olderTxTimestamp"));
        this.newerTxTimestamp = Long.valueOf(masterIndexDoc.get("newerTxTimestamp"));
        this.olderRxTimestamp = Long.valueOf(masterIndexDoc.get("olderRxTimestamp"));
        this.newerRxTimestamp = Long.valueOf(masterIndexDoc.get("newerRxTimestamp"));
        this.runStartTimestamp = Long.valueOf(masterIndexDoc.get("runStartTimestamp"));
        this.runEndTimestamp = Long.valueOf(masterIndexDoc.get("runEndTimestamp"));
        this.longTermIndexName = masterIndexDoc.get("longTermIndexName");
    }

    @Override
    public String toString() {
        return "MasterIndexRecord{" +
                "longTermIndexName='" + longTermIndexName +
                "', olderTx=" + olderTxTimestamp +
                ", newerTx=" + newerTxTimestamp +
                ", olderRx=" + olderRxTimestamp +
                ", newerRx=" + newerRxTimestamp +
                ", runStart=" + runStartTimestamp +
                ", runEnd=" + runEndTimestamp +
                '}';
    }

    public String getLongTermIndexName() {
        return longTermIndexName;
    }

}
