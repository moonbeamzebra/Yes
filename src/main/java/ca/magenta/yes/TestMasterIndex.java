package ca.magenta.yes;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;


// http://www.avajava.com/tutorials/lessons/how-do-i-use-lucene-to-index-and-search-text-files.html

public class TestMasterIndex {


    public static void main(String[] args) throws Exception {



        searchIndex("longTermIndexName:1482935703524.run.1482935673521-1482935703525.lucene");
        searchIndex("newerRxTimestamp:1482935703524");
        searchIndex("olderRxTimestamp:[1482935733788 TO 1482935794326]");






    }

    public static void searchIndex(String searchString) throws IOException, ParseException, ParseException {

        String indexNamePath = "/tmp/yes.lucene.index/master.lucene";

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
            //String key = String.format("{timestamp : %s, device : %s, source : %s, dest : %s, port : %s }",doc.get("timestamp"),doc.get("device"),doc.get("source"),doc.get("dest"),doc.get("port") );
            System.out.println("Found:" + doc.toString());
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