package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.data.LogstashMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;


public class RealTimeProcessor extends Processor {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RealTimeProcessor.class.getPackage().getName());

    public RealTimeProcessor(String name, BlockingQueue<HashMap<String, Object>> inputQueue) throws AppException {
        super(name, inputQueue);
    }

    synchronized public void createIndex(String indexPath) throws AppException{

        IndexWriter indexWriter = null;

        try {
            Analyzer analyzer = new StandardAnalyzer();

            indexDir = new RAMDirectory();

            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);


            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(),e);
        }

        this.luceneIndexWriter = indexWriter;
    }


}
