package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.Config;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.document.*;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;


public class LongTermProcessorMgmt extends ProcessorMgmt {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LongTermProcessorMgmt.class.getPackage().getName());

    private final String masterIndexPathName;

    public LongTermProcessorMgmt(String name, long cuttingTime, Config config, String partition) {
        super(name, partition, cuttingTime, config);


        logger.info(String.format("New LongTermProcessorMgmt running for partition [%s]", partition));

        masterIndexPathName = config.getIndexBaseDirectory() +
                File.separator +
                "master.lucene";

    }

    synchronized  void publishIndex(Processor longTermProcessor,
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
        if ( dir.isDirectory() ) {
            dir.renameTo(newDirName);
            updateMasterIndex(masterIndexPathName, newFileName, longTermProcessor.getRunTimeStamps());
            logger.info(String.format("Index [%s] published", newFileName));
        }
        else
        {
            logger.error(String.format("Unexpected error; [%s] is not a directory", newIndexPathName));
        }
    }

    synchronized static private void updateMasterIndex(String masterIndexPathName, String newFileName,
                                                Processor.RunTimeStamps runTimeStamps) throws IOException, AppException {
        IndexWriter masterIndex = openIndex(masterIndexPathName);

        Document document = new Document();

        document.add(new StringField("olderTxTimestamp", Long.toString(runTimeStamps.getOlderTxTimestamp()), Field.Store.YES));
//        document.add(new LongPoint("olderTxTimestamp", runTimeStamps.getOlderTxTimestamp()));
//        document.add(new StoredField("olderTxTimestamp", runTimeStamps.getOlderTxTimestamp()));
        document.add(new StringField("newerTxTimestamp", Long.toString(runTimeStamps.getNewerTxTimestamp()), Field.Store.YES));
//        document.add(new LongPoint("newerTxTimestamp", runTimeStamps.getNewerTxTimestamp()));
//        document.add(new StoredField("newerTxTimestamp", runTimeStamps.getNewerTxTimestamp()));

        document.add(new StringField("olderRxTimestamp", Long.toString(runTimeStamps.getOlderRxTimestamp()), Field.Store.YES));
//        document.add(new LongPoint("olderRxTimestamp", runTimeStamps.getOlderRxTimestamp()));
//        document.add(new StoredField("olderRxTimestamp", runTimeStamps.getOlderRxTimestamp()));
        document.add(new StringField("newerRxTimestamp", Long.toString(runTimeStamps.getNewerRxTimestamp()), Field.Store.YES));
//        document.add(new LongPoint("newerRxTimestamp", runTimeStamps.getNewerRxTimestamp()));
//        document.add(new StoredField("newerRxTimestamp", runTimeStamps.getNewerRxTimestamp()));

        document.add(new StringField("runStartTimestamp", Long.toString(runTimeStamps.getRunStartTimestamp()), Field.Store.YES));
//        document.add(new LongPoint("runStartTimestamp", runTimeStamps.getRunStartTimestamp()));
//        document.add(new StoredField("runStartTimestamp", runTimeStamps.getRunStartTimestamp()));
        document.add(new StringField("runEndTimestamp", Long.toString(runTimeStamps.getRunEndTimestamp()), Field.Store.YES));
//        document.add(new LongPoint("runEndTimestamp", runTimeStamps.getRunEndTimestamp()));
//        document.add(new StoredField("runEndTimestamp", runTimeStamps.getRunEndTimestamp()));

        document.add(new StringField("longTermIndexName", newFileName, Field.Store.YES));

        masterIndex.addDocument(document);
        masterIndex.commit();
        masterIndex.close();

    }


    synchronized  void deleteUnusedIndex(String indexPathName)
    {
        File index = new File(indexPathName);
        String[]entries = index.list();
        for(String s: entries){
            logger.info(String.format("Delete [%s]", s));
            File currentFile = new File(index.getPath(),s);
            currentFile.delete();
            logger.info(String.format("Done"));
        }
        String s = indexPathName;
        logger.info(String.format("Delete dir [%s]", indexPathName));
        index.delete();
        logger.info(String.format("Done"));
    }

    Processor createProcessor(BlockingQueue<HashMap<String, Object>> queue) throws AppException {

        return new LongTermProcessor("LongTermProcessor", partition, queue);

    }

    synchronized static public IndexWriter openIndex(String indexPath) throws AppException {

        IndexWriter indexWriter = null;

        try {
            Analyzer analyzer = new StandardAnalyzer();

            logger.debug("Indexing in '" + indexPath + "'");


            Directory indexDir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            // Add new documents to an existing index:
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(),e);
        }

        return indexWriter;
    }

}
