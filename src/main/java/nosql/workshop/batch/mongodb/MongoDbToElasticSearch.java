package nosql.workshop.batch.mongodb;

import com.mongodb.*;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Julie on 09/02/2016.
 */
public class MongoDbToElasticSearch {

    public static final String COL_INSTALLATIONS = "installations";


    public static void main(String[] args) {
        JestClient client = ESConnectionUtil.createClient("MONGOLAB_URI");

        try {
            // Si la collection existe déjà, on la supprime
            boolean indexExists = client.execute(new IndicesExists.Builder(COL_INSTALLATIONS).build()).isSucceeded();
            if (indexExists) {
                client.execute(new DeleteIndex.Builder(COL_INSTALLATIONS).build());
            }

            // Récupération des données en base MongoDb
            Map<String, DBObject> source = listInstallations();

            // Envoi sur la base ElasticSearch
            Bulk bulk = new Bulk.Builder()
                    .defaultIndex(COL_INSTALLATIONS)
                    .defaultType("installation")
                    .addAction(new Index.Builder(source).build())
                    .build();

            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static Map<String, DBObject> listInstallations() {
        DBCollection collection = getDbCollection();

        DBCursor cursor = collection.find();
        cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);

        Map<String, DBObject> source = new HashMap<String, DBObject>();

        // On récupère les 500 premiers objets
        int i = 500;
        while (cursor.hasNext() && i > 0) {
            DBObject dbObject = cursor.next();
            source.put(dbObject.get("_id").toString(), dbObject);
            i--;
        }
        cursor.close();

        return source;
    }

    private static DBCollection getDbCollection() {
        String givenUri = System.getenv("MONGOLAB_URI");
        String uri = givenUri == null ? "mongodb://localhost:27017/nosql-workshop" : givenUri;
        MongoClientURI mongoClientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(mongoClientURI);

        DB db = mongoClient.getDB(mongoClientURI.getDatabase());
        return db.getCollection(COL_INSTALLATIONS);
    }

}
