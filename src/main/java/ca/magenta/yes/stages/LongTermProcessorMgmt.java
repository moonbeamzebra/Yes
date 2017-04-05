package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndex;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;


class LongTermProcessorMgmt extends ProcessorMgmt {

    private static final Logger logger = LoggerFactory.getLogger(LongTermProcessorMgmt.class.getPackage().getName());

    //private final String masterIndexPathName;
    private final MasterIndex masterIndex;

    LongTermProcessorMgmt(MasterIndex masterIndex, String name, long cuttingTime, String partition) {
        super(name, partition, cuttingTime);

        this.masterIndex = masterIndex;
    }

    synchronized void publishIndex(Processor longTermProcessor,
                                   String indexPath,
                                   String indexPathName) throws IOException, AppException {
        String newFileName = String.format("%s-%d.run.%d-%d.lucene",
                partition,
                longTermProcessor.getRunTimeStamps().getNewerRxTimestamp(),
                longTermProcessor.getRunTimeStamps().getRunStartTimestamp(),
                longTermProcessor.getRunTimeStamps().getRunEndTimestamp());
        String newIndexPathName = indexPath + newFileName;
        File dir = new File(indexPathName);
        File newDirName = new File(newIndexPathName);
        if (dir.isDirectory()) {
            if (dir.renameTo(newDirName)) {
                masterIndex.update(newFileName, longTermProcessor.getRunTimeStamps());
                //updateMasterIndex(masterIndexPathName, newFileName, longTermProcessor.getRunTimeStamps());
                logger.info(String.format("Index [%s] published", newFileName));
            } else {
                logger.error(String.format("Cannot rename [%s to %s]", indexPathName, newIndexPathName));
            }
        } else {
            logger.error(String.format("Unexpected error; [%s] is not a directory", newIndexPathName));
        }
    }

//    synchronized static private void updateMasterIndex(String masterIndexPathName, String newFileName,
//                                                       Processor.RunTimeStamps runTimeStamps) throws IOException, AppException {
//        IndexWriter masterIndex = openIndex(masterIndexPathName);
//
//        Document document = new Document();
//
//        document.add(new SortedNumericDocValuesField("olderTxTimestamp", runTimeStamps.getOlderSrcTimestamp()));
//        document.add(new LongPoint("olderTxTimestamp", runTimeStamps.getOlderSrcTimestamp()));
//        document.add(new StoredField("olderTxTimestamp", runTimeStamps.getOlderSrcTimestamp()));
//
//        document.add(new SortedNumericDocValuesField("newerTxTimestamp", runTimeStamps.getNewerSrcTimestamp()));
//        document.add(new LongPoint("newerTxTimestamp", runTimeStamps.getNewerSrcTimestamp()));
//        document.add(new StoredField("newerTxTimestamp", runTimeStamps.getNewerSrcTimestamp()));
//
//        document.add(new SortedNumericDocValuesField("olderRxTimestamp", runTimeStamps.getOlderRxTimestamp()));
//        document.add(new LongPoint("olderRxTimestamp", runTimeStamps.getOlderRxTimestamp()));
//        document.add(new StoredField("olderRxTimestamp", runTimeStamps.getOlderRxTimestamp()));
//
//        document.add(new SortedNumericDocValuesField("newerRxTimestamp", runTimeStamps.getNewerRxTimestamp()));
//        document.add(new LongPoint("newerRxTimestamp", runTimeStamps.getNewerRxTimestamp()));
//        document.add(new StoredField("newerRxTimestamp", runTimeStamps.getNewerRxTimestamp()));
//
//        document.add(new SortedNumericDocValuesField("runStartTimestamp", runTimeStamps.getRunStartTimestamp()));
//        document.add(new LongPoint("runStartTimestamp", runTimeStamps.getRunStartTimestamp()));
//        document.add(new StoredField("runStartTimestamp", runTimeStamps.getRunStartTimestamp()));
//
//        document.add(new SortedNumericDocValuesField("runEndTimestamp", runTimeStamps.getRunEndTimestamp()));
//        document.add(new LongPoint("runEndTimestamp", runTimeStamps.getRunEndTimestamp()));
//        document.add(new StoredField("runEndTimestamp", runTimeStamps.getRunEndTimestamp()));
//
//        document.add(new StringField("longTermIndexName", newFileName, Field.Store.YES));
//
//        masterIndex.addDocument(document);
//        masterIndex.commit();
//        masterIndex.close();
//
//    }
//

    synchronized void deleteUnusedIndex(String indexPathName) {
        File index = new File(indexPathName);
        String[] entries = index.list();
        if (entries != null) {
            for (String s : entries) {
                logger.info(String.format("Delete [%s]", s));
                File currentFile = new File(index.getPath(), s);
                if (currentFile.delete())
                    logger.info("Done");
                else
                    logger.error(String.format("Cannot delete [%s/%s]", index.getPath(), s));
            }
        }
        logger.info(String.format("Delete dir [%s]", indexPathName));
        if (index.delete())
            logger.info("Done");
        else
            logger.error(String.format("Cannot delete [%s]", indexPathName));
    }


    Processor createProcessor(BlockingQueue<Object> queue) throws AppException {

        return new LongTermProcessor("LongTermProcessor", partition, queue);

    }

//    private synchronized static IndexWriter openIndex(String indexPath) throws AppException {
//
//        IndexWriter indexWriter;
//
//        try {
//            Analyzer analyzer = new StandardAnalyzer();
//
//            logger.debug("Indexing in '" + indexPath + "'");
//
//
//            Directory indexDir = NIOFSDirectory.open(Paths.get(indexPath));
//            //Directory indexDir = FSDirectory.open(Paths.get(indexPath));
//            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
//
//            // Add new documents to an existing index:
//            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
//
//            indexWriter = new IndexWriter(indexDir, iwc);
//
//        } catch (IOException e) {
//            throw new AppException(e.getMessage(), e);
//        }
//
//        return indexWriter;
//    }

}
