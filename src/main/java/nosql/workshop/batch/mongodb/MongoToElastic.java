package nosql.workshop.batch.mongodb;

import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.bson.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by romanlp on 09/02/16.
 */
public class MongoToElastic {

    private String connectionUrl = "http://localhost:9200";

    MongoCollection<Document> coll;

    public static void main(String[] args) {

        MongoToElastic obj = new MongoToElastic();
        obj.run();

    }

    public void run(){

        MongoClient mongoClient = new MongoClient();

        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        this.coll = db.getCollection("installations");
        FindIterable<Document> cursor = coll.find();
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(connectionUrl)
                .multiThreaded(true)
                .build());

        JestClient client = factory.getObject();

        Map<String, Object> source = new HashMap<>();
        source.put("installation", this.getInstallations(cursor));
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

    private Object getInstallations(FindIterable<Document> cursor) {
        return null;
    }


}
