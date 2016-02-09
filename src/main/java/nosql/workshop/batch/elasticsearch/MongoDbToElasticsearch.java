package nosql.workshop.batch.elasticsearch;

import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import nosql.workshop.model.Installation;
import nosql.workshop.resources.InstallationResource;
import org.bson.Document;

/**
 * Created by Joris on 09/02/2016.
 */
public class MongoDbToElasticsearch {

    public static void main(String[] args) {

    }

    public void readData(){
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        FindIterable<Document> iterable = db.getCollection("installations").find();

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
    }

    public void writeToElastic(){
        String connectionUrl = "http://localhost";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(connectionUrl)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();
    }
}
