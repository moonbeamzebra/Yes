package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import ca.magenta.yes.Globals;
import ca.magenta.yes.stages.LongTermProcessor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class MasterIndex {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    private static MasterIndex instance;

    private final IndexWriter indexWriter;

    private Searcher searcher = null;

    public static MasterIndex getInstance() throws AppException {
        if (instance == null) {
            instance = new MasterIndex();
        }
        return instance;
    }

    private MasterIndex() throws AppException {
        String masterIndexPathName = Globals.getConfig().getIndexBaseDirectory() +
                File.separator +
                "master.lucene";

        indexWriter = openIndexWriter(masterIndexPathName);

        searcher = Searcher.getInstance(indexWriter, false);

    }

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

    public void update(String newFileName, LongTermProcessor.RunTimeStamps runTimeStamps) throws AppException {

        Document document = new Document();

        document.add(new SortedNumericDocValuesField("olderTxTimestamp", runTimeStamps.getOlderSrcTimestamp()));
        document.add(new LongPoint("olderTxTimestamp", runTimeStamps.getOlderSrcTimestamp()));
        document.add(new StoredField("olderTxTimestamp", runTimeStamps.getOlderSrcTimestamp()));

        document.add(new SortedNumericDocValuesField("newerTxTimestamp", runTimeStamps.getNewerSrcTimestamp()));
        document.add(new LongPoint("newerTxTimestamp", runTimeStamps.getNewerSrcTimestamp()));
        document.add(new StoredField("newerTxTimestamp", runTimeStamps.getNewerSrcTimestamp()));

        document.add(new SortedNumericDocValuesField("olderRxTimestamp", runTimeStamps.getOlderRxTimestamp()));
        document.add(new LongPoint("olderRxTimestamp", runTimeStamps.getOlderRxTimestamp()));
        document.add(new StoredField("olderRxTimestamp", runTimeStamps.getOlderRxTimestamp()));

        document.add(new SortedNumericDocValuesField("newerRxTimestamp", runTimeStamps.getNewerRxTimestamp()));
        document.add(new LongPoint("newerRxTimestamp", runTimeStamps.getNewerRxTimestamp()));
        document.add(new StoredField("newerRxTimestamp", runTimeStamps.getNewerRxTimestamp()));

        document.add(new SortedNumericDocValuesField("runStartTimestamp", runTimeStamps.getRunStartTimestamp()));
        document.add(new LongPoint("runStartTimestamp", runTimeStamps.getRunStartTimestamp()));
        document.add(new StoredField("runStartTimestamp", runTimeStamps.getRunStartTimestamp()));

        document.add(new SortedNumericDocValuesField("runEndTimestamp", runTimeStamps.getRunEndTimestamp()));
        document.add(new LongPoint("runEndTimestamp", runTimeStamps.getRunEndTimestamp()));
        document.add(new StoredField("runEndTimestamp", runTimeStamps.getRunEndTimestamp()));

        document.add(new StringField("longTermIndexName", newFileName, Field.Store.YES));

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

    public Searcher getSearcher() {
        return searcher;
    }
}
