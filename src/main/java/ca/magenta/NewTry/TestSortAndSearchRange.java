package ca.magenta.NewTry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;


import java.nio.file.Paths;


public class TestSortAndSearchRange {


    public static void main(String[] args) throws Exception {

        String indexPath = "/tmp/testSort";
        Analyzer standardAnalyzer = new StandardAnalyzer();
        Directory indexDir = FSDirectory.open(Paths.get(indexPath));
        IndexWriterConfig iwc = new IndexWriterConfig(standardAnalyzer);

        Query query = null;
        String field = "age";
        QueryParser parser = new QueryParser(field, new StandardAnalyzer());

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        IndexWriter masterIndex = new IndexWriter(indexDir, iwc);

        Document doc;
        String name;
        long age;
        long ts;

        name = "bob";
        age = 20;
        doc = new Document();
        doc.add(new TextField("name", name, Field.Store.YES));
        doc.add(new SortedDocValuesField("name", new BytesRef(name)));
        doc.add(new SortedNumericDocValuesField("age", age));
        doc.add(new LongPoint("age", age));
        doc.add(new StoredField("age", age));
        ts = System.currentTimeMillis();
        doc.add(new SortedNumericDocValuesField("ts", ts));
        doc.add(new StoredField("ts", ts));
        masterIndex.addDocument(doc);
        Thread.sleep(1);

        name = "bill";
        age = 17;
        doc = new Document();
        doc.add(new TextField("name", name, Field.Store.YES));
        doc.add(new SortedDocValuesField("name", new BytesRef(name)));
        doc.add(new SortedNumericDocValuesField("age", age));
        doc.add(new LongPoint("age", age));
        doc.add(new StoredField("age", age));
        ts = System.currentTimeMillis();
        doc.add(new SortedNumericDocValuesField("ts", ts));
        doc.add(new StoredField("ts", ts));
        masterIndex.addDocument(doc);
        Thread.sleep(1);

        name = "max";
        age = 21;
        doc = new Document();
        doc.add(new TextField("name", name, Field.Store.YES));
        doc.add(new SortedDocValuesField("name", new BytesRef(name)));
        doc.add(new SortedNumericDocValuesField("age", age));
        doc.add(new LongPoint("age", age));
        doc.add(new StoredField("age", age));
        ts = System.currentTimeMillis();
        doc.add(new SortedNumericDocValuesField("ts", ts));
        doc.add(new StoredField("ts", ts));
        masterIndex.addDocument(doc);
        Thread.sleep(1);

        name = "jim";
        age = 19;
        doc = new Document();
        doc.add(new TextField("name", name, Field.Store.YES));
        doc.add(new SortedDocValuesField("name", new BytesRef(name)));
        doc.add(new SortedNumericDocValuesField("age", age));
        doc.add(new LongPoint("age", age));
        doc.add(new StoredField("age", age));
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
        docs = searcher.search(queryParser.parse("name:b*"), 100, sort);
        System.out.println("Sorted by name");
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            Document doc2 = searcher.doc(scoreDoc.doc);
            System.out.println("Name:" + doc2.get("name") + " ; age:" + doc2.get("age") + " ; ts:" + doc2.get("ts"));
        }

        sort = new Sort(new SortedNumericSortField("age", SortField.Type.LONG, false));
        query = parser.parse("age:[19 TO 20]");
        query = LongPoint.newRangeQuery("age", 19, 20);
        System.out.println("Searching for: " + query.toString());
        //docs = searcher.search(LongPoint.newRangeQuery("age", 19, 20), 100, sort);
        docs = searcher.search(query, 100, sort);
        System.out.println("Sorted by age");
        for (ScoreDoc scoreDoc : docs.scoreDocs) {

            Document doc2 = searcher.doc(scoreDoc.doc);
            System.out.println("Name:" + doc2.get("name") + " ; age:" + doc2.get("age") + " ; ts:" + doc2.get("ts"));
        }

        StandardQueryParser queryParserHelper = new StandardQueryParser();
        query = queryParserHelper.parse("name:b*", "message");


        //http://makble.com/how-to-do-term-query-in-lucene-index-example
        //TermQuery query = new TermQuery(LongPoint.newRangeQuery("age", 19, 20));
        //TermQuery query2 = new TermQuery(new Term("author", "sam"));

        StandardSyntaxParser standardSyntaxParser;


        BooleanQuery booleanQuery = new BooleanQuery.Builder().
                add(LongPoint.newRangeQuery("age", 19, 20), BooleanClause.Occur.MUST).
                add(query, BooleanClause.Occur.MUST).build();

        sort = new Sort(new SortField("name", SortField.FIELD_DOC.getType(), false));
        // name = "bob"; age = 20; should hits
        //Query query = queryParser.parse("age:[19 TO 20] AND name:b*");
        //docs = searcher.search(queryParser.parse("age:[19 TO 20] AND name:b*"), 100, sort);
        docs = searcher.search(booleanQuery, 100, sort);
        //docs = searcher.search(query, 100, sort);
        System.out.println("Sorted by index order; see ts");
        for (ScoreDoc scoreDoc : docs.scoreDocs) {

            Document doc2 = searcher.doc(scoreDoc.doc);
            System.out.println("Name:" + doc2.get("name") + " ; age:" + doc2.get("age") + " ; ts:" + doc2.get("ts"));
        }

        reader.close();

    }
}