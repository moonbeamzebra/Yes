package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import ca.magenta.yes.stages.Processor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class MasterIndexRecord {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MasterIndexRecord.class.getPackage().getName());

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

    public synchronized static void updateMasterIndex(String masterIndexPathName, String newFileName,
                                                      Processor.RunTimeStamps runTimeStamps, String partition) throws IOException, AppException {
        IndexWriter masterIndex = openIndex(masterIndexPathName);

        Document document = new Document();

        LuceneTools.storeSortedNumericDocValuesField(document, "olderTxTimestamp", runTimeStamps.getOlderSrcTimestamp());

        LuceneTools.storeSortedNumericDocValuesField(document, "newerTxTimestamp", runTimeStamps.getNewerSrcTimestamp());

        LuceneTools.storeSortedNumericDocValuesField(document, "olderRxTimestamp", runTimeStamps.getOlderRxTimestamp());

        LuceneTools.storeSortedNumericDocValuesField(document, "newerRxTimestamp", runTimeStamps.getNewerRxTimestamp());

        LuceneTools.storeSortedNumericDocValuesField(document, "runStartTimestamp", runTimeStamps.getRunStartTimestamp());

        LuceneTools.storeSortedNumericDocValuesField(document, "runEndTimestamp", runTimeStamps.getRunEndTimestamp());

        document.add(new StringField("longTermIndexName", newFileName, Field.Store.YES));

        masterIndex.addDocument(document);
        masterIndex.commit();
        masterIndex.close();

    }

    private synchronized static IndexWriter openIndex(String indexPath) throws AppException {

        IndexWriter indexWriter;

        try {
            Analyzer analyzer = new StandardAnalyzer();

            logger.debug("Indexing in '" + indexPath + "'");


            Directory indexDir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            // Add new documents to an existing index:
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }

        return indexWriter;
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
