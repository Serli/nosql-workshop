package nosql.workshop.batch.mongodb;

import com.mongodb.*;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import nosql.workshop.model.Installation;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Julie on 09/02/2016.
 */
public class MongoDbToElasticSearch {

    public static final String COL_INSTALLATIONS = "installations";
    public static final String COL_EQUIPEMENTS = "equipements";
    public static final String COL_ACTIVITES = "activites";

    public static void main(String[] args) {

        String givenUri = System.getenv("MONGOLAB_URI");
        String uri = givenUri == null ? "mongodb://localhost:27017/nosql-workshop" : givenUri;
        MongoClientURI mongoClientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(mongoClientURI);
        DB db = mongoClient.getDB(mongoClientURI.getDatabase());

        DBCollection collection = db.getCollection(COL_INSTALLATIONS);

        String connectionUrl = "http://localhost:9200";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(connectionUrl)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        DBCursor cursor = collection.find();

        Map<String, DBObject> source = new HashMap<String, DBObject>();

        try {
            Bulk.Builder bulkIndexBuilder = new Bulk.Builder();
            while(cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                source.put(dbObject.get("_id").toString(), dbObject);
            }
            boolean indexExists = client.execute(new IndicesExists.Builder(COL_INSTALLATIONS).build()).isSucceeded();
            if (indexExists) {
                client.execute(new DeleteIndex.Builder(COL_INSTALLATIONS).build());
            }
            bulkIndexBuilder.addAction(new Index.Builder(source)
             .index(COL_INSTALLATIONS)
             .type("installation")
             .build());
            client.execute(bulkIndexBuilder.build());
        } catch (IOException e) {
        e.printStackTrace();
        } finally {
            cursor.close();
        }
    }


}
