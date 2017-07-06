package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.utils.queuing.MyBlockingQueue;
import ca.magenta.yes.data.Partition;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;


public class RealTimeProcessor extends Processor {

    static final String SHORT_NAME = "RTP";

    RealTimeProcessor(ProcessorMgmt processorMgmt, Partition partition, MyBlockingQueue<Object> inputQueue, int queueDepth) throws AppException {
        super(processorMgmt, partition, inputQueue, queueDepth);
    }

    @Override
    protected String getShortName() {
        return SHORT_NAME;
    }

    public synchronized void createIndex(String indexPath) throws AppException {

        IndexWriter indexWriter;

        try {

            indexDir = new RAMDirectory();

            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());


            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }


        this.luceneIndexWriter = indexWriter;
    }


}
