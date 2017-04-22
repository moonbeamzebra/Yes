package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;


public class RealTimeProcessor extends Processor {


    RealTimeProcessor(String partition, BlockingQueue<Object> inputQueue) throws AppException {
        super(partition, inputQueue);
    }

    synchronized public void createIndex(String indexPath) throws AppException {

        IndexWriter indexWriter;

        try {
            Analyzer analyzer = new StandardAnalyzer();

            indexDir = new RAMDirectory();

            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);


            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }

        this.luceneIndexWriter = indexWriter;
    }


}
