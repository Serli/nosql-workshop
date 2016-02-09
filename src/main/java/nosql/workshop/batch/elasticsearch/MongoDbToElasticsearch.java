package nosql.workshop.batch.elasticsearch;

import com.mongodb.*;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Joris on 09/02/2016.
 */
public class MongoDbToElasticsearch {

    private MongoClient mongoClient;

    public MongoDbToElasticsearch() {
        mongoClient = new MongoClient();
    }

    public static void main(String[] args) {
        MongoDbToElasticsearch mongoDbToElasticsearch = new MongoDbToElasticsearch();
        mongoDbToElasticsearch.writeToElastic();
    }

    public List<Index> getInstallations(DBCursor cursor){
        List<Index> itemList = new ArrayList<>();

        while(cursor.hasNext()){
            itemList.add(createIndex(cursor.next()));
        }

        return itemList;
    }

    public void writeToElastic(){
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection col = db.getCollection("installations");
        DBCursor cursor = col.find();

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

    private static Index createIndex(DBObject object){
        Map<String, Object> source = new HashMap<>();
        String id = object.get("_id").toString();
        object.removeField("_id");
        source.put("installation", object);
        return new Index.Builder(object).id(id).build();

    }
}
