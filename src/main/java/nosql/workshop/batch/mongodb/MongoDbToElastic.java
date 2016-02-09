package nosql.workshop.batch.mongodb;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;
import org.bson.Document;
import org.elasticsearch.action.index.IndexResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Clémentine on 09/02/2016.
 */
public class MongoDbToElastic {



    public static void main(String[] args) {
        JestClient client= ESConnectionUtil.createClient("");
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        DB db = mongoClient.getDB( "nosql-workshop" );

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
    }

    private static List<Index> getIndexes(DBCursor cursor){
        List<Index> indexes = new ArrayList<>();
        while(cursor.hasNext()){
            indexes.add(createIndex(cursor.next()));
        }
        return indexes;
    }
    private static Index createIndex(DBObject object){
        String id = object.get("_id").toString();
        object.removeField("_id");
        object.removeField("dateMiseAJourFiche");
        return new Index.Builder(object).id(id).build();
    }

}
