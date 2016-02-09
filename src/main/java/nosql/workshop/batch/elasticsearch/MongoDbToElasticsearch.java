package nosql.workshop.batch.elasticsearch;

import com.mongodb.*;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;


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

    public List<DBObject> getInstallations(DBCursor cursor){
        List<DBObject> itemList = new ArrayList<>();

        while(cursor.hasNext()){
            itemList.add(cursor.next());
        }

        return itemList;
    }

    public void writeToElastic(){
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection col = db.getCollection("installation");
        DBCursor cursor = col.find();

        String connectionUrl = "http://localhost:9200";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(connectionUrl)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        Map<String, Object> source = new HashMap<>();
        source.put("installation", this.getInstallations(cursor));

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("installations")
                .defaultType("installation")
                .addAction(new Index.Builder(source).build())
                .build();

        try {
            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
