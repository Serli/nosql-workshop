package nosql.workshop.batch.mongodb;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class MongoDbToElasticSearch {

    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient();
        Client elasticClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress("host", 9300));

        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection installations = db.getCollection("installations");

        elasticClient.close();
        mongoClient.close();

    }

}