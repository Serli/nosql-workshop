package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static nosql.workshop.model.Installation.*;

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

    public Installation random() {
        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
    }

    public Installation getById(String id) {
        return installations.findOne("{_id: '"+id+"'}").as(Installation.class);
    }

    public List<Installation> findAll() {
        return Lists.newArrayList(installations.find().as(Installation.class).iterator());
    }

    public List<Installation> geosearch(Context context) {
        String latitude = context.get("lat");
        String longitude = context.get("lng");
        String distance = context.get("distance");

        return Lists.newArrayList(installations
                        .find("{location : { $near: {$geometry: {type:'Point', coordinates: ["+longitude+", "+latitude+"]}, $maxDistance: "+distance+"}}}")
                        .as(Installation.class).iterator());
    }

    public List<Installation> search(Context context) {

        return Lists.newArrayList(installations
                .find("{'query': {'multi_match': { 'query' :'"+context.get("query")+"', 'fields': ['_all']}}")
                .as(Installation.class).iterator());
    }

    public InstallationsStats stats() {

        return null;
    }
}
