package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.Globals;
import ca.magenta.yes.api.LongTermReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;

public class MasterIndexLucene extends MasterIndex {

    private static final Logger logger = LoggerFactory.getLogger(MasterIndexLucene.class.getSimpleName());

    private final IndexWriter indexWriter;

    private Searcher searcher = null;

    public static MasterIndex getInstance(String masterIndexEndpoint) throws AppException {
        if (instance == null) {
            instance = new MasterIndexLucene();
        }
        return instance;
    }

    private MasterIndexLucene() throws AppException {
        super();
        String masterIndexPathName = Globals.getConfig().getIndexBaseDirectory() +
                File.separator +
                "master.lucene";

        indexWriter = openIndexWriter(masterIndexPathName);

        searcher = Searcher.getInstance(indexWriter, false);

    }

    @Override
    public void close() {

        searcher.close();

        try {
            indexWriter.commit();
        } catch (IOException e) {
            logger.error("Cannot commit MasterIndex Writer", e);
        }

        try {
            indexWriter.close();
        } catch (IOException e) {
            logger.error("Cannot close MasterIndex Writer", e);
        }

    }

    private IndexWriter openIndexWriter(String indexPath) throws AppException {

        IndexWriter indexWriter;

        try {
            Analyzer analyzer = new StandardAnalyzer();

            if (logger.isDebugEnabled())
                logger.debug("Master Indexing in '" + indexPath + "'");

            Directory indexDir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }

        return indexWriter;
    }

    private Searcher getSearcher() {
        return searcher;
    }

    private void addDocument(Document document) throws AppException {
        try {
            indexWriter.addDocument(document);
            indexWriter.flush();
            indexWriter.commit();

            Searcher tSearcher = Searcher.openIfChanged(searcher);
            if (tSearcher != null) {
                searcher = tSearcher;
            }
        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }

    }

    @Override
    public void addRecord(MasterIndexRecord masterIndexRecord) throws AppException {

        addDocument(toDocument(masterIndexRecord));

    }

    @Override
    public void search(LongTermReader longTermReader,
                       String partition,
                       TimeRange periodTimeRange,
                       int limit,
                       String searchString,
                       boolean reverse,
                       PrintWriter client) throws IOException, ParseException, QueryNodeException {

        BooleanQuery bMasterSearch =
                MasterIndexLucene.buildSearchStringForTimeRangeAndPartition(Globals.DrivingTimestamp.RECEIVE_TIME,
                        partition,
                        periodTimeRange);

        logger.info("TimeRange Search String In MasterIndex: " + bMasterSearch.toString());

        // https://wiki.apache.org/lucene-java/ImproveSearchingSpeed

        IndexSearcher indexSearcher;
        Searcher searcher = this.getSearcher();
        indexSearcher = searcher.getIndexSearcher();


        int maxTotalHits = Globals.getConfig().getMaxTotalHit_MasterIndex();


        // TODO Change the following
        if (limit <= 0) limit = Integer.MAX_VALUE;

        int soFarCount = 0;
        Sort sort = MasterIndexLucene.buildSort_receiveTimeDriving(reverse);
        ScoreDoc lastScoreDoc = null;
        int totalRead = maxTotalHits; // just to let enter in the following loop
        if (indexSearcher != null) {
            while (longTermReader.isDoRun() && (totalRead >= maxTotalHits) && (soFarCount < limit)) {
                TopDocs results;

                results = indexSearcher.searchAfter(lastScoreDoc, bMasterSearch, maxTotalHits, sort);

                totalRead = results.scoreDocs.length;

                for (ScoreDoc scoreDoc : results.scoreDocs) {
                    lastScoreDoc = scoreDoc;
                    Document doc = indexSearcher.doc(scoreDoc.doc);
                    MasterIndexRecord masterIndexRecord = MasterIndexLucene.valueOf(doc);
                    soFarCount = longTermReader.searchInLongTermIndex(masterIndexRecord.getLongTermIndexName(),
                            periodTimeRange,
                            searchString,
                            reverse,
                            client,
                            limit,
                            soFarCount);
                    if ( !longTermReader.isDoRun() || !(soFarCount < limit) )
                    {
                        break;
                    }

                }
            }
        }
    }

    private static BooleanQuery buildSearchStringForTimeRangeAndPartition(Globals.DrivingTimestamp drivingTimestamp,
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


        BooleanQuery.Builder timeBooleanQueryBuilder = new BooleanQuery.Builder();
        timeBooleanQueryBuilder.add(bMasterSearchLeftPart, BooleanClause.Occur.SHOULD).
                add(bMasterSearchMiddlePart, BooleanClause.Occur.SHOULD).
                add(bMasterSearchRightPart, BooleanClause.Occur.SHOULD).
                add(bMasterSearchNarrowPart, BooleanClause.Occur.SHOULD);


        BooleanQuery.Builder returnedBooleanQueryBuilder;

        if (partition != null) {
            returnedBooleanQueryBuilder = new BooleanQuery.Builder();
            StandardQueryParser queryParserHelper = new StandardQueryParser();
            Query partitionQuery;
            try {
                partitionQuery = queryParserHelper.parse(String.format("%s:%s", PARTITION_FIELD_NAME, partition),
                        PARTITION_FIELD_NAME);
                returnedBooleanQueryBuilder.add(partitionQuery, BooleanClause.Occur.MUST);
            } catch (QueryNodeException e) {
                logger.error(e.getClass().getSimpleName(),e);
            }
            returnedBooleanQueryBuilder.add(timeBooleanQueryBuilder.build(),BooleanClause.Occur.MUST);
        }
        else
        {
            returnedBooleanQueryBuilder = timeBooleanQueryBuilder;
        }

        return returnedBooleanQueryBuilder.build();

    }

    private static Sort buildSort_receiveTimeDriving(boolean reverse) {
        return new Sort(new SortedNumericSortField(OLDER_RECEIVE_TIMESTAMP_FIELD_NAME, SortField.Type.LONG, reverse));
    }

    private static MasterIndexRecord valueOf(Document masterIndexDoc) {

        MasterIndexRecord.RuntimeTimestamps runtimeTimestamps = new MasterIndexRecord.RuntimeTimestamps(
                Long.valueOf(masterIndexDoc.get(OLDER_SOURCE_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(NEWER_SOURCE_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(OLDER_RECEIVE_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(NEWER_RECEIVE_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(RUN_START_TIMESTAMP_FIELD_NAME)),
                Long.valueOf(masterIndexDoc.get(RUN_END_TIMESTAMP_FIELD_NAME))
        );

        return new MasterIndexRecord(masterIndexDoc.get(LONG_TERM_INDEX_NAME_FIELD_NAME),
                masterIndexDoc.get(PARTITION_FIELD_NAME),
                runtimeTimestamps);
    }

    private Document toDocument(MasterIndexRecord masterIndexRecord) throws AppException {

        Document document = new Document();

        LuceneTools.storeSortedNumericDocValuesField(document, OLDER_SOURCE_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getOlderSrcTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, NEWER_SOURCE_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getNewerSrcTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, OLDER_RECEIVE_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getOlderRxTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, NEWER_RECEIVE_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getNewerRxTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, RUN_START_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getRunStartTimestamp());
        LuceneTools.storeSortedNumericDocValuesField(document, RUN_END_TIMESTAMP_FIELD_NAME, masterIndexRecord.getRuntimeTimestamps().getRunEndTimestamp());

        LuceneTools.luceneStoreNonTokenizedString(document, PARTITION_FIELD_NAME, masterIndexRecord.getPartitionName());

        document.add(new StringField(LONG_TERM_INDEX_NAME_FIELD_NAME, masterIndexRecord.getLongTermIndexName(), Field.Store.YES));

        return document;
    }



}
