package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
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

    public Installation random() {
        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
    }

    public List<Installation> list() {
        return Lists.newArrayList(installations.find("").as(Installation.class).iterator());
    }

    public Installation get(String num) {
        return installations.findOne("{_id: \"" + num + "\"}").as(Installation.class);
    }

    public List<Installation> geosearch(String lat, String lng, String distance) {
        installations.ensureIndex("{ location : '2dsphere' } ");
        return Lists.newArrayList(installations
                .find("{\"location\" : { $near : { $geometry : { type : \"Point\", coordinates: [" +
                        lng + ", " + lat + "]}, $maxDistance : " + distance + "}}}")
                .as(Installation.class).iterator());
    }

    public InstallationsStats stats() {
        InstallationsStats stats = new InstallationsStats();

        stats.setTotalCount(installations.count());

        stats.setCountByActivity(Lists.newArrayList(installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$unwind: \"$equipements.activites\"}")
                .and("{$group: {_id: \"$equipements.activites\", total:{$sum : 1}}}")
                .and("{$project: {activite: \"$_id\", total : 1}}")
                .as(CountByActivity.class).iterator()));

        IDContainer t = installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$group: {_id: \"$_id\", total:{$sum : 1}}}")
                .and("{$sort: {total : -1}}")
                .and("{$limit: 1}")
                .and("{$project : {id: \"$_id\", _id: 0}}")
                .as(IDContainer.class)
                .next();
        stats.setInstallationWithMaxEquipments(get(t.id));

        stats.setAverageEquipmentsPerInstallation(installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$group: {_id: \"$nom\", sum:{$sum : 1}}}")
                .and("{$group: {_id: 0, average:{$avg : \"$sum\"}}}")
                .as(Average.class).next().getAverage());
        return stats;
    }

    public static class IDContainer {
        public String id;
    }
}
