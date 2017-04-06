package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;


public class LongTermProcessor extends Processor {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LongTermProcessor.class.getPackage().getName());

    LongTermProcessor(String name, String partition, BlockingQueue<Object> inputQueue) throws AppException {
        super(partition, inputQueue);
    }

    synchronized public void createIndex(String indexPath) throws AppException {

        // RAMDirectory to FSDirectory
        // http://stackoverflow.com/questions/3913180/how-to-integrate-ramdirectory-into-fsdirectory-in-lucene
        // https://wiki.apache.org/lucene-java/ImproveIndexingSpeed



        IndexWriter indexWriter;

        try {
            Analyzer analyzer = new StandardAnalyzer();

            logger.info("Indexing in '" + indexPath + "'");


            indexDir = null;
            indexDir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            // Add new documents to an existing index:
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }

        this.luceneIndexWriter = indexWriter;
    }


}
