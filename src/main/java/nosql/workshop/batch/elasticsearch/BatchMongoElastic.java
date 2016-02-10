package nosql.workshop.batch.elasticsearch;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.IOException;

public class BatchMongoElastic {
    public static void main(String[] args) {
        JestClient jest = ESConnectionUtil.createClient("");
        MongoClient mongoClient = new MongoClient();
        DBCursor installations = mongoClient.getDB("nosql-workshop").getCollection("installations").find();

        String index = "installations";
        String type = "installation";
        Bulk.Builder builder = new Bulk.Builder();
        installations.forEach(installation -> builder.addAction(createIndex(installation, index, type)));
        Bulk bulk = builder.build();

        try {
            jest.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Index createIndex(DBObject installation, String index, String type) {
        String id = installation.get("_id").toString();
        installation.put("id", installation.get("_id"));
        installation.removeField("_id");
        installation.removeField("dateMiseAJourFiche");
        return new Index.Builder(installation).index(index).type(type).id(id).build();
    }
}
