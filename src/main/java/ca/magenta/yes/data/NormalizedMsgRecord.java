package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class NormalizedMsgRecord {

    public static final String RECEIVE_TIMESTAMP_FIELD_NAME = "_yes_RxTimestamp";
    public static final String SOURCE_TIMESTAMP_FIELD_NAME = "_yes_SrcTimestamp";
    public static final String MESSAGE_FIELD_NAME = "_yes_Message";
    public static final String PARTITION_FIELD_NAME = "_yes_Partition";
    public static final String MSG_TYPE_FIELD_NAME = "_yes_MsgType";
    public static final String LOGSTASH_TIMESTAMP = "@timestamp";
    public static final SimpleDateFormat LOGSTASH_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NormalizedMsgRecord.class.getName());

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");

    private final HashMap<String, Object> data;

    public NormalizedMsgRecord(Document logRecordDoc) {

        data = new HashMap<>();

        for (IndexableField field : logRecordDoc.getFields()) {
            if (logger.isDebugEnabled())
                logger.debug(String.format("field:[%s]; [%s]", field.name(), field.fieldType().toString()));
            switch (field.name()) {
                case SOURCE_TIMESTAMP_FIELD_NAME:
                case RECEIVE_TIMESTAMP_FIELD_NAME:
                    data.put(field.name(), Long.valueOf(logRecordDoc.get(field.name())));
                    break;
                default:
                    data.put(field.name(), logRecordDoc.get(field.name()));
            }
        }

    }

    private NormalizedMsgRecord(HashMap<String, Object> data) {
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
            if (!MESSAGE_FIELD_NAME.equals(entry.getKey())) {
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

    public static NormalizedMsgRecord fromJson(String jSon) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        HashMap<String, Object> data = mapper.readValue(jSon, HashMap.class);

        return new NormalizedMsgRecord(data);

    }

    private long getRxTimestamp() {
        return (long) data.get(NormalizedMsgRecord.RECEIVE_TIMESTAMP_FIELD_NAME);
    }

    public String getPrettyRxTimestamp() {
        return DATE_FORMAT.format(getRxTimestamp());
    }

    public String getPartition() {
        return data.get(NormalizedMsgRecord.PARTITION_FIELD_NAME).toString();
    }

    public String getMessage() {
        return data.get(NormalizedMsgRecord.MESSAGE_FIELD_NAME).toString();
    }



    private static int breakerSequence = -1;

    synchronized public static String generateBreakerSequence()
    {
        if (breakerSequence == Integer.MAX_VALUE)
        {
            breakerSequence = -1;
        }
        breakerSequence++;
        return String.format("%08X",breakerSequence);
    }

    public static String toStringTimestamp(long epoch)
    {
        return String.format("%020d", epoch);
    }

    public static HashMap<String, Object> initiateMsgHash(String logMsg, String msgType, String partition) {

        HashMap<String, Object> hashedMsg = new HashMap<>();

        hashedMsg.put(MESSAGE_FIELD_NAME, logMsg);
        hashedMsg.put(MSG_TYPE_FIELD_NAME, msgType);
        hashedMsg.put(PARTITION_FIELD_NAME, partition);

        return hashedMsg;

    }

    public static void store(HashMap<String, Object> message, IndexWriter luceneIndexWriter) throws AppException {
        Document document = new Document();

        for (Map.Entry<String, Object> fieldE : message.entrySet()) {
            if (NormalizedMsgRecord.MESSAGE_FIELD_NAME.equals(fieldE.getKey())) {
                document.add(new TextField(NormalizedMsgRecord.MESSAGE_FIELD_NAME, (String) fieldE.getValue(), Field.Store.YES));
            } else if (fieldE.getValue() instanceof Integer) {
                document.add(new IntPoint(fieldE.getKey(), (Integer) fieldE.getValue()));
                document.add(new SortedNumericDocValuesField(fieldE.getKey(), (Integer) fieldE.getValue()));
                document.add(new StoredField(fieldE.getKey(), (Integer) fieldE.getValue()));
            } else if (fieldE.getValue() instanceof Long) {
                document.add(new LongPoint(fieldE.getKey(), (Long) fieldE.getValue()));
                document.add(new SortedNumericDocValuesField(fieldE.getKey(), (Long) fieldE.getValue()));
                document.add(new StoredField(fieldE.getKey(), (Long) fieldE.getValue()));
                if (logger.isDebugEnabled()) {
                    long longValue = (Long) fieldE.getValue();
                    logger.debug(String.format("ADDED:[%s];[%d]", fieldE.getKey(), longValue));
                }
            } else {
                FieldType newType = new FieldType();
                newType.setTokenized(false);
                newType.setStored(true);
                newType.setIndexOptions(IndexOptions.DOCS);
                //Field f = new Field(fieldE.getKey(), (String) fieldE.getValue(), newType);
                document.add(new StringField(fieldE.getKey(), (String) fieldE.getValue(), Field.Store.YES));
            }
        }

        try {
            luceneIndexWriter.addDocument(document);
        } catch (IOException e) {
            throw new AppException(e.getClass().getSimpleName(), e);
        }

    }

    public static void initiateTimestampsInMsgHash(HashMap<String, Object> hashedMsg) {

        long epoch = System.currentTimeMillis();
        hashedMsg.put(NormalizedMsgRecord.RECEIVE_TIMESTAMP_FIELD_NAME, epoch);
        String str = (String) hashedMsg.get(NormalizedMsgRecord.LOGSTASH_TIMESTAMP);
        if (str != null) {
            Date date = null;
            try {
                date = NormalizedMsgRecord.LOGSTASH_TIMESTAMP_FORMAT.parse(str);
                epoch = date.getTime();
                if (logger.isDebugEnabled())
                    logger.debug(String.format("Logstash timestamp:[%d]", epoch));
            } catch (ParseException e) {
                logger.error(String.format("Cannot parse Logstash date: [%s]", str), e);
            }
        }
        hashedMsg.put(NormalizedMsgRecord.SOURCE_TIMESTAMP_FIELD_NAME, epoch);
    }
}
