package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {
    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public List<Installation> getAllInstallations(){
        return null;
    }

    public Installation getInstallation(String numero){
        return installations.findOne("{_id: '" + numero + "'}").as(Installation.class);
    }

    public List<Installation> searchInstallations(){
        return null;
    }

    public Installation getRandomInstallation() {
        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
    }

    public List<Installation> getInstallationByGeoSearch(float lat, float lng, int distance){
        List<Installation> geoList = new ArrayList<>();
        installations.ensureIndex("{ 'location' : '2dsphere' }");
        MongoCursor<Installation> cursor = installations.find("{'location': {$near: { $geometry: { type: 'Point', coordinates: [#, #]}, $maxDistance: #}}}", lng, lat, distance).as(Installation.class);
        cursor.forEach(e -> geoList.add(e));
        return geoList;
    }

    public InstallationsStats getStats(){
        return null;
    }
}
