package ca.magenta.NewTry;

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

import java.nio.file.Paths;


public class TestLongPointSort {


    public static void main(String[] args) throws Exception {

        String indexPath = "/tmp/testSort";
        Analyzer standardAnalyzer = new StandardAnalyzer();
        Directory indexDir = FSDirectory.open(Paths.get(indexPath));
        IndexWriterConfig iwc = new IndexWriterConfig(standardAnalyzer);

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        IndexWriter masterIndex = new IndexWriter(indexDir, iwc);

        Document doc = new Document();

        String name = "bob";
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