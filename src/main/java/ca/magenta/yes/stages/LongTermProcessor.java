package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.data.LogstashMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;


public class LongTermProcessor extends Processor {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LongTermProcessor.class.getPackage().getName());

    public LongTermProcessor(String name, String partition, BlockingQueue<Object> inputQueue) throws AppException {
        super(name, partition, inputQueue);
    }

    synchronized  public void createIndex(String indexPath) throws AppException{

        IndexWriter indexWriter = null;

        try {
            Analyzer analyzer = new StandardAnalyzer();
            boolean recreateIndexIfExists = false;

            logger.info("Indexing in '" + indexPath + "'");


            indexDir = null;
            indexDir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            if (recreateIndexIfExists) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            }

            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(),e);
        }

        this.luceneIndexWriter = indexWriter;
    }


}
