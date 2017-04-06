package ca.magenta.yes.data;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

/**
 * Created by jplaberge on 2017-03-30.
 */
public class LuceneTools {
    static void luceneStoreNonTokenizedString(Document document, String key, String value)
    {
        FieldType newType = new FieldType();
        newType.setTokenized(false);
        newType.setStored(true);
        newType.setIndexOptions(IndexOptions.DOCS);
        document.add(new StringField(key, value, Field.Store.YES));
    }

    static void luceneStoreSortedDoc(Document document, String key, String value)
    {
        //document.add(new TextField(key, value, Field.Store.YES));
        luceneStoreNonTokenizedString(document, key, value);

        document.add(new SortedDocValuesField(key, new BytesRef(value)));
    }

    public static void storeSortedNumericDocValuesField(Document document, String key, long value) {
        document.add(new SortedNumericDocValuesField(key, value));
        document.add(new LongPoint(key, value));
        document.add(new StoredField(key, value));
    }
}
