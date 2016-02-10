package nosql.workshop.batch.mongodb; /*
 * ${FILE_NAME}
 * author:   Maxime Perocheau
 * created:  2016 février 09 @ 10:23
 * modified: 2016 février 09 @ 10:23
 *
 * TODO : description
 */

import com.mongodb.*;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MongoToElastic {

    public static void main(String[] args) {

        DB db = new MongoClient().getDB( "nosql-workshop" );
        Cursor cursor = db.getCollection("installations").find();

        JestClient client = ESConnectionUtil.createClient("");

        List<Index> list = new ArrayList<>();

        while(cursor.hasNext()){
            DBObject object = cursor.next();
            String id = object.get("_id").toString();
            object.removeField("_id");
            object.put("id", id);
            list.add(new Index.Builder(object).id(id).build());
        }
        cursor.close();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("installations")
                .defaultType("installation")
                .addAction(list)
                .build();

        try {
            client.execute(bulk);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}