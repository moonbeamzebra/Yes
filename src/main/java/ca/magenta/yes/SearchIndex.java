package ca.magenta.yes;

import ca.magenta.utils.AppException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;


// http://www.avajava.com/tutorials/lessons/how-do-i-use-lucene-to-index-and-search-text-files.html

public class SearchIndex {


    public static void main(String[] args) throws Exception {


        IndexWriter masterIndex = openIndex("/tmp/allo");

        Document doc = new Document();

        Random randomGenerator = new Random();
        long randomAge = randomGenerator.nextInt(50);

        String term = "allo";
        doc.add(new TextField("title", term, Field.Store.YES));
        doc.add(new SortedDocValuesField("title", new BytesRef(term)));
        //doc.add(new LongPoint("age", randomAge));
        doc.add(new SortedNumericDocValuesField("age", randomAge));
        doc.add(new StoredField("age1", randomAge));

        masterIndex.addDocument(doc);
        masterIndex.commit();
        masterIndex.close();

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get("/tmp/allo")));
        IndexSearcher searcher = new IndexSearcher(reader);

//        Sort sort = new Sort(new SortField("title", SortField.Type.STRING));
//        TopDocs docs = searcher.search(new TermQuery(new Term("title", "allo")), 10, sort);

//        Sort sort = new Sort(new SortField("age", SortField.Type.LONG));
//        Analyzer analyzer = new KeywordAnalyzer();
//        QueryParser queryParser = new QueryParser("message", analyzer);
//        Query query = queryParser.parse("age:[8 TO 10]");
//        TopDocs docs = searcher.search(query, 10, sort);

        //Sort sort = new Sort(new SortField("age", SortField.Type.LONG));
        //SortField longSort = new SortedNumericSortField("age", SortField.Type.LONG, true);
        //Sort sort = new Sort(longSort);
        Sort sort = new Sort(new SortField("age", SortField.Type.LONG, false));
        Analyzer analyzer = new KeywordAnalyzer();
        QueryParser queryParser = new QueryParser("message", analyzer);
        //Query query = queryParser.parse("age:[8 TO 10]");
        TopDocs docs = searcher.search(LongPoint.newRangeQuery("age",0L,51L), 10, sort);


        for (ScoreDoc scoreDoc : docs.scoreDocs) {

            //System.out.println(String.format("scoreDocs:[%s]",scoreDocs.toString()));
            Document doc2 = searcher.doc(scoreDoc.doc);
            //String key = String.format("{timestamp : %s, device : %s, source : %s, dest : %s, port : %s }",doc.get("timestamp"),doc.get("device"),doc.get("source"),doc.get("dest"),doc.get("port") );
            //System.out.println("Found:" + doc.toString());
            //NormalizedMsgRecord normalizedLogRecord = new NormalizedMsgRecord(doc);
            //System.out.println("Found:" + normalizedLogRecord.toString());


            System.out.println("getValues:" + doc2.get("title") + " ; " + doc2.getValues("age").toString());
            System.out.println("Found:" + doc2.get("title") + " ; " + doc2.get("age"));

        }

        reader.close();


// JPL
//        searchIndex("device:fw01");
//        searchIndex("10.10.10.10");
//        searchIndex("dropped");
//        searchIndex("accepted");
//        searchIndex("terminated");





    }

    synchronized static public IndexWriter openIndex(String indexPath) throws AppException {

        IndexWriter indexWriter = null;

        try {
            Analyzer analyzer = new StandardAnalyzer();

            //logger.debug("Indexing in '" + indexPath + "'");


            Directory indexDir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            // Add new documents to an existing index:
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

            indexWriter = new IndexWriter(indexDir, iwc);

        } catch (IOException e) {
            throw new AppException(e.getMessage(),e);
        }

        return indexWriter;
    }


    public static void searchIndex(String searchString) throws IOException, ParseException, ParseException {

        String indexName = "lucene";
        String indexNamePath = "." + File.separator + indexName;
        indexNamePath = "./luceneIndex/1482345024631-1482345024631.cut.1482434631483.lucene";

        System.out.println("Searching for '" + searchString + "' in " +  indexNamePath);
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexNamePath)));
        IndexSearcher searcher = new IndexSearcher(reader);


        Analyzer analyzer = new KeywordAnalyzer();
        //Analyzer analyzer = new StandardAnalyzer();
        QueryParser queryParser = new QueryParser("message", analyzer);
        Query query = queryParser.parse(searchString);
        TopDocs results = searcher.search(query, 1000);
        System.out.println("Number of hits: " + results.totalHits);
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            //System.out.println(String.format("scoreDocs:[%s]",scoreDocs.toString()));
            Document doc = searcher.doc(scoreDoc.doc);
            String key = String.format("{timestamp : %s, device : %s, source : %s, dest : %s, port : %s }",doc.get("timestamp"),doc.get("device"),doc.get("source"),doc.get("dest"),doc.get("port") );
            System.out.println("Found:" + key);
        }

        reader.close();

        //deleteDirFile(new File(searchIndexDirectory));

    }




    public static void deleteDirFile(File file)
            throws IOException{

        if(file.isDirectory()){

            //directory is empty, then delete it
            if(file.list().length==0){

                file.delete();
                System.out.println("Directory is deleted : "
                        + file.getAbsolutePath());

            }else{

                //list all the directory contents
                String files[] = file.list();

                for (String temp : files) {
                    //construct the file structure
                    File fileDelete = new File(file, temp);

                    //recursive delete
                    deleteDirFile(fileDelete);
                }

                //check the directory again, if empty then delete it
                if(file.list().length==0){
                    file.delete();
                    System.out.println("Directory is deleted : "
                            + file.getAbsolutePath());
                }
            }

        }else{
            //if file, then delete it
            file.delete();
            System.out.println("File is deleted : " + file.getAbsolutePath());
        }
    }

}