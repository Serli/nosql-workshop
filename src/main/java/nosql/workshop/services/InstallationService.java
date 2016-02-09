package nosql.workshop.services;

import com.google.inject.Singleton;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.util.List;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {

    private static final String DB_NAME = "nosql-workshop";
    private static final String COLLECTION_NAME = "installations";
    private final MongoCollection collection;

    public InstallationService(){
        MongoClient client = new MongoClient();
        DB db = client.getDB(DB_NAME);
        Jongo jongo = new Jongo(db);
        collection = jongo.getCollection(COLLECTION_NAME);
    }

    public List<Installation> getAllInstallations(){
        return null;
    }

    public Installation getInstallation(String numero){
        Installation res = new Installation();
        res = collection.findOne("{_id: '" + numero + "'}").as(Installation.class);
        return res;
    }

    public List<Installation> searchInstallations(){
        return null;
    }

    public Installation getRandomInstallation(){
        return null;
    }

    public List<Installation> getInstallationByGeoSearch(){
        return null;
    }

    public InstallationsStats getStats(){
        return null;
    }
}
