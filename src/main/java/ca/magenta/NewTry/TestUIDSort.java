package ca.magenta.NewTry;

import ca.magenta.yes.data.NormalizedMsgRecord;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import javax.persistence.criteria.CriteriaBuilder;
import java.nio.file.Paths;


public class TestUIDSort {


    public static void main(String[] args) throws Exception {

        String indexPath = "/tmp/testSort";
        Analyzer standardAnalyzer = new StandardAnalyzer();
        Directory indexDir = FSDirectory.open(Paths.get(indexPath));
        IndexWriterConfig iwc = new IndexWriterConfig(standardAnalyzer);

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        IndexWriter masterIndex = new IndexWriter(indexDir, iwc);

        Document doc = new Document();

        long max = Long.MAX_VALUE;
        System.out.println("MAX: [" + max + "]");
        System.out.println(String.format("Max: [%020d]", max));
        System.out.println(String.format("Max: [%020X]", max));
        System.out.println("MAX+1: [" + ++max + "]");

        int imax = Integer.MAX_VALUE;
        System.out.println("iMAX: [" + imax + "]");
        System.out.println(String.format("iMax: [%020d]", imax));
        System.out.println(String.format("iMax: [%08X]", imax));
        System.out.println(String.format("iMax8: [%08X]", 8));
        System.out.println("iMAX+1: [" + ++imax + "]");

        String now;
        String now1 = String.format("%020d", System.currentTimeMillis());
        Thread.sleep(1);
        String now2 = String.format("%020d", System.currentTimeMillis());
        now2 = now1;
        Thread.sleep(1);
        String now3 = String.format("%020d", System.currentTimeMillis());
        now3 = now1;
        String breakerSequence;
        String name;

        name = "bob";
        now =  String.format("%020d", System.currentTimeMillis());
        breakerSequence = NormalizedMsgRecord.generateBreakerSequence();
        //breakerSequence = String.format("%08X",4);
        doc.add(new TextField("rxTimestamp", now3, Field.Store.YES));
        doc.add(new SortedDocValuesField("rxTimestamp", new BytesRef(now3)));
        doc.add(new TextField("breakerSequence", breakerSequence, Field.Store.YES));
        doc.add(new SortedDocValuesField("breakerSequence", new BytesRef(breakerSequence)));
        doc.add(new TextField("name", name, Field.Store.YES));
        doc.add(new SortedDocValuesField("name", new BytesRef(name)));
        doc.add(new SortedNumericDocValuesField("age", 20L));
        doc.add(new StoredField("age", 20L));
        long ts = System.currentTimeMillis();
        doc.add(new SortedNumericDocValuesField("ts", ts));
        doc.add(new StoredField("ts", ts));
        masterIndex.addDocument(doc);
        Thread.sleep(1);

        name = "max";
        doc = new Document();
        //now =  NormalizedMsgRecord.returnNow();
        breakerSequence = NormalizedMsgRecord.generateBreakerSequence();
        //breakerSequence = String.format("%08X",2);
        doc.add(new TextField("rxTimestamp", now2, Field.Store.YES));
        doc.add(new SortedDocValuesField("rxTimestamp", new BytesRef(now2)));
        doc.add(new TextField("breakerSequence", breakerSequence, Field.Store.YES));
        doc.add(new SortedDocValuesField("breakerSequence", new BytesRef(breakerSequence)));
        doc.add(new TextField("name", name, Field.Store.YES));
        doc.add(new SortedDocValuesField("name", new BytesRef(name)));
        doc.add(new SortedNumericDocValuesField("age", 19L));
        doc.add(new StoredField("age", 19L));
        ts = System.currentTimeMillis();
        doc.add(new SortedNumericDocValuesField("ts", ts));
        doc.add(new StoredField("ts", ts));
        masterIndex.addDocument(doc);
        Thread.sleep(1);

        name = "jim";
        doc = new Document();
        //now =  NormalizedMsgRecord.returnNow();
        breakerSequence = NormalizedMsgRecord.generateBreakerSequence();
        //breakerSequence = String.format("%08X",3);
        doc.add(new TextField("rxTimestamp", now1, Field.Store.YES));
        doc.add(new SortedDocValuesField("rxTimestamp", new BytesRef(now1)));
        doc.add(new TextField("breakerSequence", breakerSequence, Field.Store.YES));
        doc.add(new SortedDocValuesField("breakerSequence", new BytesRef(breakerSequence)));
        doc.add(new TextField("name", name, Field.Store.YES));
        doc.add(new SortedDocValuesField("name", new BytesRef(name)));
        doc.add(new SortedNumericDocValuesField("age", 21L));
        doc.add(new StoredField("age", 21L));
        ts = System.currentTimeMillis();
        doc.add(new SortedNumericDocValuesField("ts", ts));
        doc.add(new StoredField("ts", ts));
        masterIndex.addDocument(doc);

        masterIndex.commit();
        masterIndex.close();

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher searcher = new IndexSearcher(reader);

        Analyzer analyzer = new KeywordAnalyzer();
        QueryParser queryParser = new QueryParser("message", analyzer);

        Sort sort;
        TopDocs docs;

        sort = new Sort(new SortField("rxTimestamp", SortField.Type.STRING), new SortField("breakerSequence", SortField.Type.STRING));
        docs = searcher.search(new MatchAllDocsQuery(), 100, sort);
        System.out.println("Sorted by ID");
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            Document doc2 = searcher.doc(scoreDoc.doc);
            System.out.println(
                            "rxTimestamp: " + doc2.get("rxTimestamp") +
                            " ; breakerSequence: " + doc2.get("breakerSequence") +
                            " ; Name:" + doc2.get("name") +
                            " ; age:" + doc2.get("age") +
                            " ; ts:" + doc2.get("ts"));
        }


        sort = new Sort(new SortField("name", SortField.Type.STRING));
        docs = searcher.search(new MatchAllDocsQuery(), 100, sort);
        System.out.println("Sorted by name");
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            Document doc2 = searcher.doc(scoreDoc.doc);
            System.out.println("Name:" + doc2.get("name") + " ; age:" + doc2.get("age") + " ; ts:" + doc2.get("ts"));
        }

        //docs = searcher.search(new MatchAllDocsQuery(), 100, new Sort(new SortField("age", SortField.Type.SCORE, true)));
        //sort = new Sort(new SortedNumericSortField("age", SortField.Type.LONG, true));
        //docs = searcher.search(new MatchAllDocsQuery(), 100, sort);
        sort = new Sort(new SortedNumericSortField("age", SortField.Type.LONG, false));
        docs = searcher.search(new MatchAllDocsQuery(), 100, sort);
        //docs = searcher.search(new MatchAllDocsQuery(), 100, new Sort(new SortField("age", SortField.Type.LONG, true)));
        System.out.println("Sorted by age");
        for (ScoreDoc scoreDoc : docs.scoreDocs) {

            Document doc2 = searcher.doc(scoreDoc.doc);
            System.out.println("Name:" + doc2.get("name") + " ; age:" + doc2.get("age") + " ; ts:" + doc2.get("ts"));
        }

        sort = new Sort(new SortField("name", SortField.FIELD_DOC.getType(), false));
        docs = searcher.search(new MatchAllDocsQuery(), 100, sort);
        System.out.println("Sorted by index order; see ts");
        for (ScoreDoc scoreDoc : docs.scoreDocs) {

            Document doc2 = searcher.doc(scoreDoc.doc);
            System.out.println("Name:" + doc2.get("name") + " ; age:" + doc2.get("age") + " ; ts:" + doc2.get("ts"));
        }

        reader.close();

    }
}