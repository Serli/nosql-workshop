package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
@SuppressWarnings("unchecked")
public class InstallationService {

    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
    }

    public Installation get(String numero) {
        return installations.findOne("{ _id : # }", numero).as(Installation.class);
    }

    public List<Installation> list() {
        MongoCursor<Installation> cursor = installations.find().as(Installation.class);
        List<Installation> list = new ArrayList<>();
        cursor.forEach(installation -> list.add(installation));
        return list;
    }

    /**
     * Locates installations near a given location
     *
     * @param lat latitude
     * @param lng longitude
     * @param dist max distance from point
     * @return a list of Installations
     */
    public List<Installation> near(double lat, double lng, int dist) {
        List<Installation> list = new ArrayList<>();
        installations.ensureIndex("{ location : '2dsphere' } ");
        final MongoCursor<Installation> cursor = installations.find(
                "{ location : { $near : { $geometry : { type : 'Point', coordinates : [ #, # ] }, $maxDistance : # } } }",
                lng, lat, dist).as(Installation.class);
        cursor.forEach(e -> list.add(e)); // reads the cursor into a list
        return list;
    }

}
