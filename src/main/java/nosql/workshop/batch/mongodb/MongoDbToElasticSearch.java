package nosql.workshop.batch.mongodb;

import com.mongodb.*;
import com.mongodb.util.JSON;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

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
        Index index = new Index.Builder(JSON.serialize(cursor)).index("installations").type("installation").build();
        try {
            elasticClient.execute(index);
        } catch (IOException ex) {
            System.out.println(ex.toString());
        }

        // Close MongoDB connection
        mongoClient.close();

    }

}