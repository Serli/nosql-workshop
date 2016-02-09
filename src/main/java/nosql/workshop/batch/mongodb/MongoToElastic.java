package nosql.workshop.batch.mongodb; /*
 * ${FILE_NAME}
 * author:   Maxime Perocheau
 * created:  2016 février 09 @ 10:23
 * modified: 2016 février 09 @ 10:23
 *
 * TODO : description
 */

import com.google.gson.GsonBuilder;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import org.bson.BSON;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoToElastic {

    MongoClient mongoClient;

    public MongoToElastic() {
        this.mongoClient = new MongoClient();
    }

    public static void main(String[] args) {

        MongoToElastic test = new MongoToElastic();

        DB db = test.mongoClient.getDB( "nosql-workshop" );
        Cursor cursor = db.getCollection("installations").find();

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        List<DBObject> list = new ArrayList<>();
        while(cursor.hasNext()){
            list.add(cursor.next());
        }
        cursor.close();

        Map<String, Object> source = new HashMap<String, Object>();
        source.put("installation", list);
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("installations")
                .defaultType("installation")
                .addAction(new Index.Builder(source).build())
                .build();

        try {
            client.execute(bulk);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}