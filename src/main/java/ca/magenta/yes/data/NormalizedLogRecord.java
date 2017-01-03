package ca.magenta.yes.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.Document;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Created by jplaberge on 2016-12-31.
 */
public class NormalizedLogRecord {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");

    private long srcTimestamp;
    private long rxTimestamp;

    private String deviceName;
    private String partition;
    private String source;
    private String dest;
    private String port;
    private String action;
    private String description;

    private String type;

    private String message;

    public NormalizedLogRecord(Document logRecordDoc) {
        this.srcTimestamp = Long.valueOf(logRecordDoc.get("txTimestamp"));
        this.rxTimestamp = Long.valueOf(logRecordDoc.get("rxTimestamp"));
        this.deviceName = logRecordDoc.get("device");
        this.partition = logRecordDoc.get("customer");
        this.source = logRecordDoc.get("source");
        this.dest = logRecordDoc.get("dest");
        this.port = logRecordDoc.get("port");
        this.action = logRecordDoc.get("action");
        this.description = logRecordDoc.get("msg");
        this.type = logRecordDoc.get("type");

        this.message = logRecordDoc.get("message");
    }

    public NormalizedLogRecord() {
    }

    @Override
    public String toString() {
        return "{" +
                "srcTimestamp=" + srcTimestamp +
                ", rxTimestamp=" + rxTimestamp +
                ", deviceName='" + deviceName + '\'' +
                ", partition='" + partition + '\'' +
                ", source='" + source + '\'' +
                ", dest='" + dest + '\'' +
                ", port='" + port + '\'' +
                ", action='" + action + '\'' +
                ", description='" + description + '\'' +
                ", type='" + type + '\'' +
                ", message='" + message + '\'' +
                '}';
    }

    public String toTTYString(boolean withOriginalMessage, boolean oneLiner) {


        String ttyString = String.format("%s" +
                        ", deviceName='" + deviceName + '\'' +
                        ", partition='" + partition + '\'' +
                        ", source='" + source + '\'' +
                        ", dest='" + dest + '\'' +
                        ", port='" + port + '\'' +
                        ", action='" + action + '\'' +
                        ", description='" + description + '\'' +
                        ", type='" + type + "'",
                prettyRxTimestamp());

        if (withOriginalMessage)
        {
            if (oneLiner)
            {
                ttyString = ttyString + ", message='" + message + "'";
            }
            else
            {
                ttyString = ttyString + "\n  " + message;
            }
        }

        return ttyString;
    }

    public String toJson() throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        return  mapper.writeValueAsString(this);
    }

    public static NormalizedLogRecord fromJson(String jSon) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        return mapper.readValue(jSon, NormalizedLogRecord.class);

    }


    public long getSrcTimestamp() {
        return srcTimestamp;
    }

    public String prettySrcTimestamp() {
        return DATE_FORMAT.format(srcTimestamp);
    }

    public long getRxTimestamp() {
        return rxTimestamp;
    }

    public String prettyRxTimestamp() {
        return DATE_FORMAT.format(rxTimestamp);
    }

    public String getDeviceName() {
        return deviceName;
    }

    public String getPartition() {
        return partition;
    }

    public String getSource() {
        return source;
    }

    public String getDest() {
        return dest;
    }

    public String getPort() {
        return port;
    }

    public String getAction() {
        return action;
    }

    public String getDescription() {
        return description;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

}
