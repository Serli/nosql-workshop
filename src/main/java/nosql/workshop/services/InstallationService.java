package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.searchbox.client.JestClient;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import nosql.workshop.utils.JestConnection;
import org.jongo.MongoCollection;

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
    private final JestClient elasticClient;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
        this.elasticClient = JestConnection.createClient();
    }

    public Installation random() {
        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
    }

    public Installation get(String numero) {
        return installations.findOne("{ _id : # }", numero).as(Installation.class);
    }

    public List<Installation> list() {
        return Lists.newArrayList(installations.find().as(Installation.class).iterator());
    }

    /**
     * Locates installations near a given location
     *
     * @param lat  latitude
     * @param lng  longitude
     * @param dist max distance from point
     * @return a list of Installations
     */
    public List<Installation> near(double lat, double lng, int dist) {
        List<Installation> list = new ArrayList<>();
        installations.ensureIndex("{ location : '2dsphere' } ");
        return Lists.newArrayList(installations.find(
                "{ location : { $near : { $geometry : { type : 'Point', coordinates : [ #, # ] }, $maxDistance : # } } }",
                lng, lat, dist).as(Installation.class).iterator());
    }

    public InstallationsStats stats() {
        InstallationsStats stats = null;

        return stats;

    }
}
