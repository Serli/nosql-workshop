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

    /**
     * Randomly fetch an installation from the database.
     * @return a random Installation
     */
    public Installation random() {
        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
    }

    /**
     * Fetch an installation from its id
     * @param numero the installation id
     * @return an Installation
     */
    public Installation get(String numero) {
        return installations.findOne("{ _id : # }", numero).as(Installation.class);
    }

    /**
     * Fetch all the installations in the database
     * @return a List of Installations
     */
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

        /* * CountByActivity *
         * We first unwind the $equipement field, then for each resulting document we unwind the $equipements.activites field.
         * This will produce a document for each activity in each $equipement array for each installation,
         * We then group those documents using the $equipement.activites field, and for each document we increment the $total field.
         * At that point, the result would be a list of distinct documents for each activity label along with the total counter.
         * We then project those results into the form { activite : 'tennis', total : 20 } and sort them by descending order.
         */
        final ArrayList<CountByActivity> countByActivities = Lists.newArrayList(installations.aggregate("{ $unwind : '$equipements' }")
                .and("{ $unwind : '$equipements.activites' }")
                .and("{ $group : { _id : '$equipements.activites', total : { $sum : 1 } } }")
                .and("{ $project :  { _id : 0, activite : '$_id', total : 1 } }")
                .and("{ $sort : { total : -1 } }").as(CountByActivity.class).iterator());
        stats.setCountByActivity(countByActivities);

        /* * Total count *
         * Simply does a db.installations.count()
         */
        stats.setTotalCount(installations.count());

        /* * AverageEquipmentsPerInstallation * - Solution 1
         * We first unwind the $equipement field. This will produce a document for each equipment in each installation.
         * We then group those documents by installation, counting the number of documents in each group in the $total field.
         * Lastly, we calculate the average of the $total field for all the documents, by grouping on _id : 0, meaning by grouping on nothing,
         * and project the results into the expected form.
         *
         * NB :  This solution is less efficient (it will create an unnecessary number of documents) than the solution using a $project.
         *
         *   final Average averageEquipementsPerInstallation = installations.aggregate("{ $unwind : '$equipements' }")
         *           .and("{ $group : { _id : '$_id', total : { $sum : 1 } } }")
         *           .and("{ $group : { _id : 0, average : { $avg : '$total' } } }")
         *           .and("{ $project : { _id : 0, average : 1 } }").as(Average.class).next();
         *   stats.setAverageEquipmentsPerInstallation(averageEquipementsPerInstallation.getAverage());
        */

        /* * AverageEquipmentsPerInstallation * - Solution 2
         * This solution uses the $project stage to store in a calculated field the size of each $equipement array using the $size function.
         * We then group those document by _id : 0 (grouping by nothing) and calculate the average of the $count field using the $avg function.
         * Finally, we project the result into the expected form.
         */
        final Average averageEquipementsPerInstallation = installations.aggregate("{ $project : { count : { $size : '$equipements' } } }")
                .and("{ $group : { _id : 0, average : { $avg : '$count' } } }")
                .and("{ $project : { _id : 0, average : 1 } }").as(Average.class).next();
        stats.setAverageEquipmentsPerInstallation(averageEquipementsPerInstallation.getAverage());

        /* InstallationWithMaxEquipments *
         * We use a projection to calculate the size of each $equipement array into a new $nbEquip field.
         * We then sort the result on that field by descending order, limiting to one document.
         * This will produce a result looking like { _id : "123456", nbEquip : 27 }.
         * We then retrieve the installation with the retrieved id.
         */
        Installation installationWithMaxEquipements = installations.aggregate("{ $project : { nbEquip : { $size: '$equipements' } } }")
                .and("{ $sort : { nbEquip : -1 } }")
                .and("{ $limit : 1 }").as(Installation.class).next();
        installationWithMaxEquipements = this.get(installationWithMaxEquipements.get_id());
        stats.setInstallationWithMaxEquipments(installationWithMaxEquipements);

        return stats;

    }
}
