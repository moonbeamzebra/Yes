package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Searcher {

    private static final Logger logger = LoggerFactory.getLogger(Searcher.class.getSimpleName());

    private DirectoryReader directoryReader = null;

    private final IndexWriter indexWriter;
    private final IndexSearcher indexSearcher;

    private static Searcher instance = null;

    private Searcher(IndexWriter indexWriter) throws AppException {

        super();
        logger.warn("WARN: Searcher::Searcher called!");
        this.indexWriter = indexWriter;
        try {
            this.directoryReader = DirectoryReader.open(this.indexWriter);
        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }
        indexSearcher = new IndexSearcher(directoryReader);
    }

    public IndexSearcher getIndexSearcher() {
        return indexSearcher;
    }

    synchronized public static Searcher getInstance(IndexWriter indexWriter, boolean renew) throws AppException {

        logger.warn("WARN: Searcher::getInstance called!");

        if ( renew || (instance == null) ) {
            instance = new Searcher(indexWriter);
        }

        return instance;
    }

    synchronized public static Searcher openIfChanged(Searcher searcher) throws AppException {

        Searcher rSearcher = null;
        try {
            DirectoryReader reader = DirectoryReader.openIfChanged(searcher.directoryReader);
            if (reader != null)
            {
                instance = getInstance(searcher.indexWriter, true);
                rSearcher = instance;
            }
        } catch (IOException e) {
            throw new AppException(e.getMessage(), e);
        }

        return rSearcher;
    }

    void close() {

        logger.warn("WARN: Searcher::close called!");

        if (directoryReader != null) {
            try {
                directoryReader.close();
            } catch (IOException e) {
                logger.error("Exception closing Searcher", e);
            }
            directoryReader = null;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        logger.warn("WARN: Searcher::finalize called!");
        close();
    }

}
