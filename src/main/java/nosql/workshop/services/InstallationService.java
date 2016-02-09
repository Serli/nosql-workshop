package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.List;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {

    private final MongoCollection installations;
    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public List<Installation> getAllInstallations(){
        return null;
    }

    public Installation getInstallation(String numero){
        Installation res = new Installation();
        res = installations.findOne("{_id: '" + numero + "'}").as(Installation.class);
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
