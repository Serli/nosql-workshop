package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.ArrayList;
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
        Iterator<Installation> it = installations.find().as(Installation.class).iterator();
        List<Installation> result = new ArrayList<>();
        while (it.hasNext()) {
            result.add(it.next());
        }

        return result;
    }

    public Installation getById(String id) {
        return installations.findOne("{_id:'" + id + "'}").as(Installation.class);
    }

    public List<Installation> get(String query) {

        return null;
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
        Iterator<Equipement> equipementIterator = installations.find("{equipements:1}").projection("{equipements:#}", 1).as(Equipement.class).iterator();
        double sum = 0;
        while (equipementIterator.hasNext()) {
            equipementIterator.next();
            sum++;
        }
        double averageEquipmentsPerInstallation = sum / totalCount;

        // count by activity
        List<CountByActivity> countByActivityList = new ArrayList<>();
        List<String> activities = new ArrayList<>();
        installations.find("{activites:1}").projection("{activites:#}", 1).as(String.class).iterator().forEachRemaining(activities::add);
        for (String activity : activities) {
            // get number of installations with the current activity
            long total = installations.find("{activites: {$in:['" + activity + "']}}").as(Installation.class).count();

            // create corresponding countbyactivity
            CountByActivity countByActivity = new CountByActivity();
            countByActivity.setActivite(activity);
            countByActivity.setTotal(total);
            countByActivityList.add(countByActivity);
        }


        InstallationsStats stats = new InstallationsStats();
        stats.setAverageEquipmentsPerInstallation(averageEquipmentsPerInstallation);
        stats.setTotalCount(totalCount);
        stats.setInstallationWithMaxEquipments(installationWithMaxEquipements);
        stats.setCountByActivity(countByActivityList);
        return stats;
    }

    public List<Installation> getGeoSearchResults(String lat, String lng, String distance) {
        List<Installation> results = new ArrayList<>();
        installations.find(String.format("{location:{$near:{$geometry:{type:'Point', coordinates: [%s, %s]}, $maxDistance: %s}}}", String.valueOf(lat), String.valueOf(lng), String.valueOf(distance))).as(Installation.class).forEach(results::add);
        return results;
    }
}
