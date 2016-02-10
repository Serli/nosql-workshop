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
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {
    private static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = new MongoDB().getJongo().getCollection(COLLECTION_NAME);
    }

    public List<Installation> getAll() {
        return Lists.newArrayList(installations.find().as(Installation.class).iterator());
    }

    public Installation getById(String id) {
        return installations.findOne("{_id:'" + id + "'}").as(Installation.class);
    }

    public Installation getRandom() {
        // ok for this exercise because we don't insert so many documents
        int count = (int)installations.count();
        Random rnd = new Random(System.currentTimeMillis());
        // nextInt choose between 0 and bound excluded, then we want to skip n-1 docs to choose the nth
        int nbToSkip = rnd.nextInt(count);
        Iterator<Installation> it = installations.find().skip(nbToSkip).as(Installation.class).iterator();
        // by construction, there is necessarily something in next()
        return it.next();
    }

    public InstallationsStats getStats() {
        long totalCount = installations.count();

        // installation with max equipments
        Iterator<Installation> installationIterator = installations.find().as(Installation.class).iterator();
        Installation installationWithMaxEquipements = installationIterator.next();
        Installation currentInstallation;
        while (installationIterator.hasNext()) {
            currentInstallation = installationIterator.next();
            if (currentInstallation.getEquipements().size() > installationWithMaxEquipements.getEquipements().size()) {
                installationWithMaxEquipements = currentInstallation;
            }
        }

        // average
        double averageEquipmentsPerInstallation = installations
                .aggregate("{$project:{count:{$size:'$equipements'}}}")
                .and("{$group:{_id:'0', average:{$avg:'$count'}}}")
                .as(Average.class).next().getAverage();

        // count by activity
        List<CountByActivity> countByActivityList = Lists.newArrayList(
                installations.aggregate("{$unwind: '$equipements'}")
                .and("{$unwind: '$equipements.activites'}")
                .and("{$group: {_id: '$equipements.activites', total:{$sum : 1}}}")
                .and("{$project: {activite: '$_id', total : 1}}")
                .and("{$sort: {total: -1}}")
                .as(CountByActivity.class).iterator());


        InstallationsStats stats = new InstallationsStats();
        stats.setAverageEquipmentsPerInstallation(averageEquipmentsPerInstallation);
        stats.setTotalCount(totalCount);
        stats.setInstallationWithMaxEquipments(installationWithMaxEquipements);
        stats.setCountByActivity(countByActivityList);
        return stats;
    }

    public List<Installation> getGeoSearchResults(String lat, String lng, String distance) {
        return Lists.newArrayList(installations.find(
                String.format("{location:{$near:{$geometry:{type:'Point', coordinates: [%s, %s]}, $maxDistance: %s}}}",
                        String.valueOf(lng),
                        String.valueOf(lat),
                        String.valueOf(distance)))
                .as(Installation.class).iterator());
    }
}
