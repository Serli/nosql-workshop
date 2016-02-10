package nosql.workshop.batch.mongodb;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Gwénaël on 09/02/2016.
 */
public class Mongo2ElasticSearch {
    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("nosql-workshop");

        DBCursor cursor = db.getCollection("installations").find();

        JestClient client = ESConnectionUtil.createClient("");

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("installations")
                .defaultType("installation")
                .addAction(getInstallations(cursor))
                .build();

        try {
            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<Index> getInstallations(DBCursor cursor) {
        ArrayList<Index> list = new ArrayList<>();
        while (cursor.hasNext()) {
            list.add(createIndex(cursor.next()));
        }
        return list;
    }

    private static Index createIndex(DBObject object) {
        String id = object.get("_id").toString();
        object.removeField("_id");
        object.removeField("dateMiseAJourFiche");
        return new Index.Builder(object).id(id).build();
    }
}
