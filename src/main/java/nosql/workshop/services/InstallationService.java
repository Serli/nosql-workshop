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

    /**
     * Computes statistics on the installations
     * @return InstallationStats a statistics object
     */
    public InstallationsStats stats() {
        InstallationsStats stats = new InstallationsStats();

        final ArrayList<CountByActivity> countByActivities = Lists.newArrayList(installations.aggregate("{ $unwind : '$equipements' }")
                .and("{ $unwind : '$equipements.activites' }")
                .and("{ $group : { _id : '$equipements.activites', total : { $sum : 1 } } }")
                .and("{ $project :  { _id : 0, activite : '$_id', total : 1 } }")
                .and("{ $sort : { total : -1 } }").as(CountByActivity.class).iterator());
        stats.setCountByActivity(countByActivities);

        stats.setTotalCount(installations.count());

        final Average averageEquipementsPerInstallation = installations.aggregate("{$unwind : '$equipements'}")
                .and("{ $group : { _id : '$_id', total : { $sum : 1 } } }")
                .and("{ $group : { _id : 0, average : { $avg : '$total' } } }")
                .and("{ $project : { _id : 0, average : 1 } }").as(Average.class).next();
        stats.setAverageEquipmentsPerInstallation(averageEquipementsPerInstallation.getAverage());

        Installation installationWithMaxEquipements = installations.aggregate("{ $project : { nbEquip : { $size: '$equipements' } } }")
                .and("{ $sort : { nbEquip : -1 } }")
                .and("{ $limit : 1 }").as(Installation.class).next();
        installationWithMaxEquipements = installations.findOne("{ _id : # }", installationWithMaxEquipements.get_id()).as(Installation.class);
        stats.setInstallationWithMaxEquipments(installationWithMaxEquipements);

        return stats;

    }
}
