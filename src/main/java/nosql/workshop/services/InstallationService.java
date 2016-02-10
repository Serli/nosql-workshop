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
        InstallationsStats installationsStats = new InstallationsStats();

        installationsStats.setTotalCount(installations.count());

        installationsStats.setAverageEquipmentsPerInstallation(
                installations
                        .aggregate("{$group: {_id: null, average: {$avg : {$size: \"$equipements\"}}}}")
                        .as(Average.class)
                        .next()
                        .getAverage());

        installationsStats.setCountByActivity(Lists.newArrayList(
                installations
                        .aggregate("{ $unwind: \"$equipements\" }")
                        .and("{ $unwind: \"$equipements.activites\"}")
                        .and("{ $group: {_id: \"$equipements.activites\", total:{$sum : 1}} }")
                        .and("{ $sort : { total : -1 } }")
                        .and("{ $project: {activite: \"$_id\", total : 1} }")
                        .as(CountByActivity.class).iterator()));

        installationsStats.setInstallationWithMaxEquipments(
                installations
                        .aggregate("{ $project : {_id : 1, nom: 1, equipements : 1, size : {$size: \"$equipements\"} } }")
                        .and("{ $sort : {size : -1} } }")
                        .and("{ $limit : 1 }")
                        .as(Installation.class).next()
        );

        return installationsStats;
    }

    public List<Installation> geosearch(Double lat, Double lng, Integer distance) {
        installations.ensureIndex( "{ location : \"2dsphere\" }");
        return Lists.newArrayList(
                installations
                .find("{location : { $near : { $geometry : { type : \"Point\", coordinates : [ " + lng + ", " +
                        lat + " ]}, $maxDistance : " + distance + "}}}")
                .as(Installation.class).iterator()
        );
    }

}
