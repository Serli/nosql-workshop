package nosql.workshop.batch.mongodb;

import com.google.inject.Inject;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.services.MongoDB;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.io.IOException;

/**
 * Created by SamNag on 10/02/2016.
 */
public class MongoDbToElasticSearch {


    public static void main(String[] args) throws IOException {

        // Jongo client for installations
        MongoDB mongoDB = new MongoDB();
        MongoCollection installations = mongoDB.getJongo().getCollection("installations");

        // Get the jest client
        JestClient client = ESConnectionUtil.getJestClient();

        // Get all installations and index them in ES using Jest
        MongoCursor<Installation> all = installations.find().as(Installation.class);
        Index index = null;
        // TODO filtrer les dates pour éviter les problèmes de conversion
        try {
            while(all.hasNext()) {
                Installation inst = all.next();
                index = new Index.Builder(inst).index("installations").type("installation").id(inst.get_id()).build();
                client.execute(index);
            }
        } finally {
            all.close();
        }
    }

}
