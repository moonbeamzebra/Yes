package ca.magenta.yes.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class NormalizedLogRecord {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");

    private final HashMap<String, Object> data;

    public NormalizedLogRecord(Document logRecordDoc) {

        data = new HashMap<>();

        for (IndexableField field : logRecordDoc.getFields()) {
            if (logger.isDebugEnabled())
                logger.debug(String.format("field:[%s]; [%s]", field.name(), field.fieldType().toString()));
            switch (field.name()) {
                case "srcTimestamp":
                case "rxTimestamp":
                    data.put(field.name(), Long.valueOf(logRecordDoc.get(field.name())));
                    break;
                default:
                    data.put(field.name(), logRecordDoc.get(field.name()));
            }
        }

    }

    private NormalizedLogRecord(HashMap<String, Object> data) {
        this.data = data;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append('{');

        int n = 0;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (n != 0) sb.append(',');
            n++;
            sb.append(entry.getKey());
            sb.append('=');
            sb.append(entry.getKey());
        }

        sb.append("}");

        return sb.toString();
    }

    public String toTTYString(boolean withOriginalMessage, boolean oneLiner) {

        StringBuilder sb = new StringBuilder();

        sb.append(getPrettyRxTimestamp());

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!"rxTimestamp".equals(entry.getKey())) {
                sb.append(',');
                sb.append(entry.getKey());
                sb.append("='");
                sb.append(entry.getValue());
                sb.append("'");
            }
        }

        if (withOriginalMessage) {
            if (oneLiner) {
                sb.append(", message='").append(getMessage()).append("'");
            } else {
                sb.append("\n  ").append(getMessage());
            }
        }

        return sb.toString();
    }

    public String toJson() throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        return mapper.writeValueAsString(data);
    }

    public static NormalizedLogRecord fromJson(String jSon) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        HashMap<String, Object> data = mapper.readValue(jSon, HashMap.class);

        return new NormalizedLogRecord(data);

    }

    private long getRxTimestamp() {
        return (long) data.get("rxTimestamp");
    }

    public String getPrettyRxTimestamp() {
        return DATE_FORMAT.format(getRxTimestamp());
    }

    public String getPartition() {
        return data.get("partition").toString();
    }

    public String getMessage() {
        return data.get("message").toString();
    }

}
