package nosql.workshop.batch.elasticsearch;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.mapping.PutMapping;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.IOException;

public class BatchMongoElastic {
    public static void main(String[] args) {
        JestClient client = ESConnectionUtil.createClient("");
        try {
            String index = "installations";
            String type = "installation";
            String source = "\"mappings\": {\n" +
                    "        \"installation\": {\n" +
                    "            \"properties\": {\n" +
                    "                \"location\": {\n" +
                    "                    \"properties\": {\n" +
                    "                        \"coordinates\": {\n" +
                    "                            \"type\": \"geo_point\"\n" +
                    "                        }\n" +
                    "                    }\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }";

            PutMapping putMapping = new PutMapping.Builder(index, type, source).build();
            client.execute(putMapping);

            MongoClient mongoClient = new MongoClient();
            DBCursor installations = mongoClient.getDB("nosql-workshop").getCollection("installations").find();

            Bulk.Builder builder = new Bulk.Builder();
            installations.forEach(installation -> builder.addAction(createIndex(installation, index, type)));
            Bulk bulk = builder.build();
            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            client.shutdownClient();
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
