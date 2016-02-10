package nosql.workshop.batch.elasticsearch;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Théophile Morin & Remy Ferre
 */
public class MongoDbToElasticSearch {


    public static void main(String[] args) {

        //Connect to ElasticSearch
        final JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(400000)
                .build());
        JestClient client= factory.getObject();

        //Connect to MongoDB
        MongoClient mongoClient=new MongoClient("localhost",27017);
        DB db = mongoClient.getDB("nosql-workshop");

        //Get the iterator over MongoDB elements & build the bulk to run in ElasticSearch
        DBCursor cursor=db.getCollection("installations").find();
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("installations")
                .defaultType("installation")
                .addAction(getIndexes(cursor))
                .build();
        try {
            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Batch successfully executed.");
    }

    /**
     * Iterate over the cursor to build a List of Index
     * @param cursor - The DBcursor
     * @return - The index List
     */
    private static List<Index> getIndexes(DBCursor cursor){
        List<Index> indexes = new ArrayList<>();
        while(cursor.hasNext()){
            indexes.add(createIndex(cursor.next()));
        }
        return indexes;
    }

    /**
     * Create elastic Index from DBObject
     * @param object - The DBObject
     * @return an ElasticSearch index
     */
    private static Index createIndex(DBObject object){
        String id = object.get("_id").toString();
        object.removeField("_id");
        //object.removeField("dateMiseAJourFiche");
        return new Index.Builder(object).id(id).build();
    }
}

