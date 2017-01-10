package ca.magenta.yes.connector.common;

import ca.magenta.yes.data.NormalizedLogRecord;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.log4j.Logger;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author jean-paul.laberge <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-12-04
 */
public abstract class IndexSubscriber implements Runnable {


    public static Logger logger = Logger.getLogger(IndexSubscriber.class);

    private String name = null;
    private final String searchString;

    private BlockingQueue<Directory> queue = null;

    private volatile boolean doRun = true;

    public void stop() {
        doRun = false;
    }

    public IndexSubscriber(String name, String searchString) {
        this.name = name;
        this.searchString = searchString;
        //ShortTermProcessorMgmt.indexPublisher().subscribe(this);
        RealTimeProcessorMgmt.indexPublisher().subscribe(this);
    }

    public void store(Directory indexNamePath) throws InterruptedException {
        //logger.info(String.format("IndexSubscriber receive [%s]", indexNamePath));
        queue.put(indexNamePath);
    }

    public void run() {

        logger.debug("New IndexSubscriber " + name + " running");
        queue = new ArrayBlockingQueue<Directory>(300000);
        try {
            while (doRun) {
                //Directory indexNamePath = queue.take();
                //logger.info(String.format("Received: [%s]", indexNamePath));
                Directory index = queue.take();

                /////////////
                IndexReader reader = null;
                try {
                    reader = DirectoryReader.open(index);
                    //reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
                    IndexSearcher searcher = new IndexSearcher(reader);


                    //Analyzer analyzer = new KeywordAnalyzer();
                    Analyzer analyzer = new StandardAnalyzer();
                    QueryParser queryParser = new QueryParser("message", analyzer);
                    Query query = queryParser.parse(searchString);
                    TopDocs results = searcher.search(query, 1000);
                    if (logger.isDebugEnabled())
                        logger.debug("Number of hits: " + results.totalHits);
                    for (ScoreDoc scoreDoc : results.scoreDocs) {
                        //System.out.println(String.format("scoreDocs:[%s]",scoreDocs.toString()));
                        Document doc = searcher.doc(scoreDoc.doc);
                        NormalizedLogRecord normalizedLogRecord = new NormalizedLogRecord(doc);
                        //System.out.println("Found:" + normalizedLogRecord.toString());

                        this.forward(normalizedLogRecord.toJson());
                    }

                    reader.close();


                    //deleteDirFile(new File(searchIndexDirectory));
                } catch (ParseException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                ///////////////


            }
        } catch (InterruptedException e) {
            //logger.error("InterruptedException", e);
        }
        RealTimeProcessorMgmt.indexPublisher().unsubscribe(this);
        queue.clear();
        queue = null;
        logger.debug("IndexSubscriber " + name + " stops running; queue emptied");
    }

    protected abstract void forward(String message);

    public String getName() {
        return name;
    }

}
