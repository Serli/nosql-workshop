package nosql.workshop.batch.elasticsearch;

import nosql.workshop.services.MongoDB;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;

/**
 * Created by Théophile Morin & Remy Ferre
 */
public class MongoDbToElasticSearch {

    public static void main(String[] args) throws UnknownHostException{

        MongoDB db = new MongoDB();
        Jongo jongo = db.getJongo();
        MongoCollection installations = jongo.getCollection("installations");

       /* JestClient jest = new Jes

        installations.find().as(Installation.class)
                .forEach();
*/

    }
}
