package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class NormalizedMsgRecord {

    private static final String VAR_NAME_PREFIX = ".";
    private static final String VAR_NAME_POSTFIX = ".";

    private static final String UID_FIELD_NAME = VAR_NAME_PREFIX + "uid" + VAR_NAME_POSTFIX;
    private static final String RECEIVE_TIMESTAMP_FIELD_NAME = VAR_NAME_PREFIX + "rxTimestamp" + VAR_NAME_POSTFIX;
    private static final String SOURCE_TIMESTAMP_FIELD_NAME = VAR_NAME_PREFIX + "srcTimestamp" + VAR_NAME_POSTFIX;
    private static final String MESSAGE_FIELD_NAME = VAR_NAME_PREFIX + "message" + VAR_NAME_POSTFIX;
    private static final String PARTITION_FIELD_NAME = VAR_NAME_PREFIX + "partition" + VAR_NAME_POSTFIX;
    private static final String MSG_TYPE_FIELD_NAME = VAR_NAME_PREFIX + "msgType" + VAR_NAME_POSTFIX;
    private static final String LOGSTASH_TIMESTAMP = "@timestamp";
    private static final SimpleDateFormat LOGSTASH_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NormalizedMsgRecord.class.getName());

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");

    private final HashMap<String, Object> data;
    private final long rxTimestamp;
    private final long srcTimestamp;
    private final String uid;
    private final String partition;
    private final String message;
    private final String msgType;

    //private final static ObjectMapper mapper = new ObjectMapper();


    public NormalizedMsgRecord(Document logRecordDoc) {

        long tRxTimestamp = 0;
        long tSrcTimestamp = 0;
        String tUid = "";
        String tPartition = "";
        String tMessage = "";
        String tMsgType = "";

        data = new HashMap<>();

        for (IndexableField field : logRecordDoc.getFields()) {
            if (logger.isDebugEnabled())
                logger.debug(String.format("field:[%s]; [%s]", field.name(), field.fieldType().toString()));
            switch (field.name()) {
                case RECEIVE_TIMESTAMP_FIELD_NAME:
                    tRxTimestamp = Long.valueOf(logRecordDoc.get(field.name()));
                    break;
                case SOURCE_TIMESTAMP_FIELD_NAME:
                    tSrcTimestamp = Long.valueOf(logRecordDoc.get(field.name()));
                    break;
                case UID_FIELD_NAME:
                    tUid = logRecordDoc.get(field.name());
                    break;
                case PARTITION_FIELD_NAME:
                    tPartition = logRecordDoc.get(field.name());
                    break;
                case MESSAGE_FIELD_NAME:
                    tMessage = logRecordDoc.get(field.name());

                    break;
                case MSG_TYPE_FIELD_NAME:
                    tMsgType = logRecordDoc.get(field.name());
                    break;
                default:
                    data.put(field.name(), logRecordDoc.get(field.name()));
            }
        }
        rxTimestamp = tRxTimestamp;
        srcTimestamp = tSrcTimestamp;
        uid = tUid;
        partition = tPartition;
        message = tMessage;
        msgType = tMsgType;
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("rxTimestamp:[%d]", rxTimestamp));
            logger.debug(String.format("srcTimestamp:[%d]", srcTimestamp));
            logger.debug(String.format("uid:[%s]", uid));
            logger.debug(String.format("partition:[%s]", partition));
            logger.debug(String.format("message:[%s]", message));
            logger.debug(String.format("msgType:[%s]", msgType));
        }
    }

    public NormalizedMsgRecord(ObjectMapper mapper, String jsonMsg, boolean isInitPhase) throws IOException {

        data = mapper.readValue(jsonMsg, HashMap.class);

        if (data.isEmpty()) {
            logger.error(String.format("NormalizedMsgRecord::NormalizedMsgRecord DataPart is empty ; isInitPhase:[%b]", isInitPhase));
        }

        long epoch = System.currentTimeMillis();

        message = data.get(MESSAGE_FIELD_NAME).toString();
        data.remove(MESSAGE_FIELD_NAME);
        partition = data.get(PARTITION_FIELD_NAME).toString();
        data.remove(PARTITION_FIELD_NAME);
        msgType = data.get(MSG_TYPE_FIELD_NAME).toString();
        data.remove(MSG_TYPE_FIELD_NAME);

        if (isInitPhase) {
            rxTimestamp = epoch;
            uid = generateUID(epoch);

            String str = (String) data.get(NormalizedMsgRecord.LOGSTASH_TIMESTAMP);
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
            srcTimestamp = epoch;
        } else {
            rxTimestamp = (long) data.get(RECEIVE_TIMESTAMP_FIELD_NAME);
            data.remove(RECEIVE_TIMESTAMP_FIELD_NAME);
            srcTimestamp = (long) data.get(SOURCE_TIMESTAMP_FIELD_NAME);
            data.remove(SOURCE_TIMESTAMP_FIELD_NAME);
            uid = data.get(UID_FIELD_NAME).toString();
            data.remove(UID_FIELD_NAME);
        }
    }

    public void store(IndexWriter luceneIndexWriter) throws AppException {
        Document document = new Document();

        LuceneTools.luceneStoreSortedDoc(document, UID_FIELD_NAME, uid);

        LuceneTools.luceneStoreSortedDoc(document, RECEIVE_TIMESTAMP_FIELD_NAME, toStringTimestamp(rxTimestamp));
        //LuceneTools.storeSortedLongNumericDocValuesField(document, RECEIVE_TIMESTAMP_FIELD_NAME, rxTimestamp);
        LuceneTools.luceneStoreSortedDoc(document, SOURCE_TIMESTAMP_FIELD_NAME, toStringTimestamp(srcTimestamp));
        //LuceneTools.storeSortedLongNumericDocValuesField(document, SOURCE_TIMESTAMP_FIELD_NAME, srcTimestamp);

        document.add(new TextField(MESSAGE_FIELD_NAME, message, Field.Store.YES));
        LuceneTools.luceneStoreNonTokenizedString(document, PARTITION_FIELD_NAME, partition);
        LuceneTools.luceneStoreNonTokenizedString(document, MSG_TYPE_FIELD_NAME, msgType);


        if (!data.isEmpty()) {
            for (Map.Entry<String, Object> fieldE : data.entrySet()) {
                String key = fieldE.getKey();

                if (fieldE.getValue() instanceof Integer) {
                    LuceneTools.storeSortedIntNumericDocValuesField(document, key, (Integer) fieldE.getValue());
                } else if (fieldE.getValue() instanceof Long) {
                    LuceneTools.storeSortedLongNumericDocValuesField(document, key, (Long) fieldE.getValue());
                    if (logger.isDebugEnabled()) {
                        long longValue = (Long) fieldE.getValue();
                        logger.debug(String.format("ADDED:[%s];[%d]", key, longValue));
                    }
                } else {
                    LuceneTools.luceneStoreNonTokenizedString(document, fieldE.getKey(), (String) fieldE.getValue());
                }
            }
        } else {
            //logger.warn(String.format("NormalizedMsgRecord::store DataPart is empty in [%s]", getUid()));
        }

        try {
            luceneIndexWriter.addDocument(document);
        } catch (IOException e) {
            throw new AppException(e.getClass().getSimpleName(), e);
        }

    }

    @Override
    public String toString() {

        return toJson(new ObjectMapper());
    }

    public String toRawString(boolean withOriginalMessage, boolean oneLiner) {

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

    public String toJson(ObjectMapper mapper) {

        StringBuilder sb = new StringBuilder();
        sb.append('{').append(embeddedToJson());

        if (!data.isEmpty()) {

            String dataPart = null;
            try {
                dataPart = mapper.writeValueAsString(data);
                if (logger.isDebugEnabled()) logger.debug(String.format("DataPart: [%s]", dataPart));
            } catch (JsonProcessingException e) {
                logger.error(e.getClass().getSimpleName(), e);
            }

            if ((dataPart != null) && (dataPart.length() > 2)) {
                sb.append(',').append(dataPart.substring(1));
            } else {
                logger.error(String.format("NormalizedMsgRecord::toJson Data Part [%s] was NULL or empty in [%s]", dataPart, getUid()));
                sb.append('}');
            }
        } else {
            //logger.warn(String.format("NormalizedMsgRecord::toJson Data Part is empty in [%s]", getUid()));
            sb.append('}');
        }

        return sb.toString();
    }

    private String embeddedToJson() {

        /*
        private final long rxTimestamp;
        private final long srcTimestamp;
        private final String uid;
        private final String partition;
        private final String message;
        private final String msgType;
         */

        StringBuilder sb = new StringBuilder();

        sb.append('"').append(UID_FIELD_NAME).append("\":\"").append(uid).append("\",");
        sb.append('"').append(RECEIVE_TIMESTAMP_FIELD_NAME).append("\":").append(rxTimestamp).append(",");
        sb.append('"').append(SOURCE_TIMESTAMP_FIELD_NAME).append("\":").append(srcTimestamp).append(",");
        sb.append('"').append(PARTITION_FIELD_NAME).append("\":\"").append(partition).append("\",");
        sb.append('"').append(MSG_TYPE_FIELD_NAME).append("\":\"").append(msgType).append("\",");
        sb.append('"').append(MESSAGE_FIELD_NAME).append("\":\"").append(message).append('"');

        return sb.toString();
    }

    public String getPrettyRxTimestamp() {
        return DATE_FORMAT.format(getRxTimestamp());
    }

    private static String generateUID(long timestamp) {
        return String.format("%s-%s", toStringTimestamp(timestamp), generateBreakerSequence());
    }

    public static String toStringTimestamp(long epoch) {
        return String.format("%013d", epoch);
    }

    private static int breakerSequence = -1;

    synchronized public static String generateBreakerSequence() {
        if (breakerSequence == Integer.MAX_VALUE) {
            breakerSequence = -1;
        }
        breakerSequence++;
        return String.format("%08X", breakerSequence);
    }

    public static HashMap<String, Object> initiateMsgHash(long receiveTime, String msg, String partitionName, ParsedMessage parsedMessage) {

        HashMap<String, Object> hashedMsg = parsedMessage.getMessageHashed();

        hashedMsg.put(MESSAGE_FIELD_NAME, msg);
        hashedMsg.put(MSG_TYPE_FIELD_NAME, parsedMessage.getType());
        hashedMsg.put(PARTITION_FIELD_NAME, partitionName);

        return hashedMsg;

    }

    @Deprecated
    public static HashMap<String, Object> initiateMsgHash(String logMsg, String msgType, String partition) {

        HashMap<String, Object> hashedMsg = new HashMap<>();

        hashedMsg.put(MESSAGE_FIELD_NAME, logMsg);
        hashedMsg.put(MSG_TYPE_FIELD_NAME, msgType);
        hashedMsg.put(PARTITION_FIELD_NAME, partition);

        return hashedMsg;

    }

    public long getRxTimestamp() {
        return rxTimestamp;
    }

    public long getSrcTimestamp() {
        return srcTimestamp;
    }

    public String getUid() {
        return uid;
    }

    public String getPartition() {
        return partition;
    }

    public String getMessage() {
        return message;
    }

    public String getMsgType() {
        return msgType;
    }

    public static Query buildQuery_messageAsDefaultField(String searchString) throws QueryNodeException {
        StandardQueryParser queryParserHelper = new StandardQueryParser();
        return queryParserHelper.parse(searchString, NormalizedMsgRecord.MESSAGE_FIELD_NAME);
    }

    public static Sort buildSort_uidDriving(boolean reverse) {
        return new Sort(new SortField(UID_FIELD_NAME, SortField.Type.STRING, reverse));
    }

    public static String forgeTempIndexName(String basePath, String relativePath, String prefix) {

        return basePath +
                File.separator +
                relativePath +
                File.separator +
                prefix + "." +
                java.util.UUID.randomUUID();
    }

    public static String forgePublishedFileName(Partition partition, MasterIndexRecord.RuntimeTimestamps runtimeTimestamps) {

        String newFileName = String.format("%s-r%d-r%d-s%d-s%d.run.%d-%d.lucene",
                partition.getInstanceName(),
                runtimeTimestamps.getOlderRxTimestamp(),
                runtimeTimestamps.getNewerRxTimestamp(),
                runtimeTimestamps.getOlderSrcTimestamp(),
                runtimeTimestamps.getNewerSrcTimestamp(),
                runtimeTimestamps.getRunStartTimestamp(),
                runtimeTimestamps.getRunEndTimestamp());

        return newFileName;
//        String todayAndNewFileName = relativePath + File.separator + newFileName;
//
//        return todayAndNewFileName;


    }
}
