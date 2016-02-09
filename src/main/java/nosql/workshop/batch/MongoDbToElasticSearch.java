package nosql.workshop.batch;

import com.mongodb.*;
import com.mongodb.util.JSON;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * @author Killian
 */
@SuppressWarnings("unchecked")
public class MongoDbToElasticSearch {

    public static void main(String[] args) {

        // Connect MongoDB
        MongoClient mongoClient = new MongoClient();

        // Connect ElasticSearch
        JestClient elasticClient = ESConnectionUtil.createClient();

        // Get installations from MongoDB
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection installations = db.getCollection("installations");
        DBCursor cursor = installations.find(new BasicDBObject());
        cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);

        // Creates bulk builder
        Bulk.Builder bulkBuilder = new Bulk.Builder()
                .defaultIndex("installations")
                .defaultType("installation");
        // Iterates over documents
        cursor.forEach(installation -> {
            String id = (String) installation.get("_id");
            Map installationMap = installation.toMap();
            installationMap.remove("_id");
            installationMap.put("id", id);
            installationMap.remove("dateMiseAJourFiche");
            // Add action to add current document in ElasticSearch
            System.out.println(JSON.serialize(installationMap));
            bulkBuilder.addAction(
                    new Index.Builder(
                            JSON.serialize(installationMap)
                    ).id(id).build()
            );
        });

        // Insert data into ElasticSearch
        try {
            elasticClient.execute(bulkBuilder.build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Close connections
        mongoClient.close();
        elasticClient.shutdownClient();

    }

}