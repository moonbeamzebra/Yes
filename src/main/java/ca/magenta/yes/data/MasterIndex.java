package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.api.LongTermReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;

public abstract class MasterIndex {

    private static final Logger logger = LoggerFactory.getLogger(MasterIndex.class.getSimpleName());

    static final String OLDER_SOURCE_TIMESTAMP_FIELD_NAME = "olderSrcTimestamp";
    static final String NEWER_SOURCE_TIMESTAMP_FIELD_NAME = "newerSrcTimestamp";
    static final String OLDER_RECEIVE_TIMESTAMP_FIELD_NAME = "olderRxTimestamp";
    static final String NEWER_RECEIVE_TIMESTAMP_FIELD_NAME = "newerRxTimestamp";
    static final String RUN_START_TIMESTAMP_FIELD_NAME = "runStartTimestamp";
    static final String RUN_END_TIMESTAMP_FIELD_NAME = "runEndTimestamp";
    static final String PARTITION_FIELD_NAME = "partition";
    static final String LONG_TERM_INDEX_NAME_FIELD_NAME = "longTermIndexName";

    static MasterIndex instance;

    abstract public void close();

    abstract public void addRecord(MasterIndexRecord masterIndexRecord) throws AppException;

    abstract public void search(LongTermReader longTermReader,
                       String indexBaseDirectory,
                       String partition,
                       TimeRange periodTimeRange,
                       int limit,
                       String searchString,
                       boolean reverse,
                       PrintWriter client) throws IOException, ParseException, QueryNodeException, AppException;

}
