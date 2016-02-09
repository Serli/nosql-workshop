package nosql.workshop.batch;

import com.mongodb.*;
import com.mongodb.util.JSON;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * @author Killian
 */
public class MongoDbToElasticSearch {

    public static void main(String[] args) {

        // Connect MongoDB
        MongoClient mongoClient = new MongoClient();

        // Connect ElasticSearch
        JestClientFactory jestFactory = new JestClientFactory();
        jestFactory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient elasticClient = jestFactory.getObject();

        // Get installations from MongoDB
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection installations = db.getCollection("installations");
        DBCursor cursor = installations.find(new BasicDBObject(), new BasicDBObject("dateMiseAJourFiche", "0"));

        // Insert installations in ElasticSearch
        DBObject installation;
        String id;
        Map installationMap;
        while (cursor.hasNext()) {
            installation = cursor.next();
            id = (String) installation.get("_id");
            installationMap = installation.toMap();
            installationMap.remove("_id");
            try {
                elasticClient.execute(
                        new Index.Builder(
                                JSON.serialize(installationMap)
                        ).index("installations").type("installation").id(id).build()
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        // Close connections
        mongoClient.close();
        elasticClient.shutdownClient();

    }

}