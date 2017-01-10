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

/**
 * Created by jplaberge on 2016-12-31.
 */
public class NormalizedLogRecord {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");

    private final HashMap<String, Object> data;

    public NormalizedLogRecord(Document logRecordDoc) {

        data = new HashMap<String, Object>();

        for (IndexableField field : logRecordDoc.getFields()) {
            //logger.info(String.format("field:[%s]; [%s]", field.name(), field.fieldType().toString()));
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

    public NormalizedLogRecord() {
        data = new HashMap<String, Object>();
    }

    public NormalizedLogRecord(HashMap<String, Object> data) {
        this.data = data;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append('{');

        int n = 0;
        for (Map.Entry<String, Object> entry : data.entrySet())
        {
            if (n!=0) sb.append(',');
            n++;
            sb.append(entry.getKey());
            sb.append('=');
            sb.append(entry.getKey().toString());
        }

        sb.append("}");

        return sb.toString();
    }

    public String toTTYString(boolean withOriginalMessage, boolean oneLiner) {

        StringBuilder sb = new StringBuilder();

        sb.append(prettyRxTimestamp());

        for (Map.Entry<String, Object> entry : data.entrySet())
        {
            if ( ! "rxTimestamp".equals(entry.getKey())) {
                sb.append(',');
                sb.append(entry.getKey().toString());
                sb.append("='");
                sb.append(entry.getValue());
                sb.append("'");
            }
        }

        if (withOriginalMessage)
        {
            if (oneLiner)
            {
                sb.append( ", message='" + getMessage() + "'");
            }
            else
            {
                sb.append( "\n  " + getMessage());
            }
        }

        return sb.toString();
    }

    public String toJson() throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        return  mapper.writeValueAsString(data);
    }

    public static NormalizedLogRecord fromJson(String jSon) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        HashMap<String, Object> data = mapper.readValue(jSon, HashMap.class);

        return new NormalizedLogRecord(data);

    }

    public long getRxTimestamp() {
        return (long) data.get("rxTimestamp");
    }

    public String prettyRxTimestamp() {
        return DATE_FORMAT.format(getRxTimestamp());
    }

    public String getPartition() {
        return data.get("partition").toString();
    }

    public String getMessage() {
        return data.get("message").toString();
    }

}
