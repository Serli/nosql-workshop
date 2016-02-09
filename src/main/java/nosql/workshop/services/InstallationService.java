package nosql.workshop.services;

import com.google.common.collect.Lists;
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
    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {;
        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
    }

    public List<Installation> list() {
        return Lists.newArrayList(installations.find("").as(Installation.class).iterator());
    }

    public Installation get(String numero) {
        return installations.findOne("{_id: '" + numero + "'}").as(Installation.class);
    }

    public InstallationsStats stats() {
        return new InstallationsStats();
    }

    public Double[] getLocation(String townName) {
        return new Double[]{3.4, 3.2};
    }
}
