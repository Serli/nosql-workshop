package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.MongoCollection;

import net.codestory.http.Context;
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

    public Installation getByNumero(String numero) {
        return installations.findOne("{_id: '" + numero + "'}").as(Installation.class);
    }

    public List<Installation> getList() {
        return Lists.newArrayList(installations.find().as(Installation.class).iterator());
    }

    public List<Installation> geosearch(String lat, String lng, String distance) {
         return Lists.newArrayList(installations.find("{'location' : { $near : { $geometry : { type : 'Point', coordinates: ["+lng+", "+lat+"]}, $maxDistance : "+distance+"}}}")
                .as(Installation.class).iterator());
    }

    public InstallationsStats stats() {
        InstallationsStats stats = new InstallationsStats();
        stats.setTotalCount(
                installations.count()
        );
        /*stats.setInstallationWithMaxEquipments(
                installations.aggregate("{$unwind: '$equipements'}")
                    .and("$group: {_id: '$nom', sum: {$sum: 1}))")
                    .and("")
        );*/

        stats.setAverageEquipmentsPerInstallation(
                installations.aggregate("{$unwind: '$equipements'}")
                        .and("$group: {_id: '$nom', sum: {$sum: 1}))")
                        .and("$group: {_id: 0, avg: {'$avg' : '$sum'").as(Average.class).next().getAverage());

        stats.setCountByActivity(Lists.newArrayList(installations.aggregate("{$unwind:'$equipements'}")
                .and("{$unwind: '$equipements.activites'}")
                .and("{$group: {_id: '$equipements.activites', total:{$sum : 1}}}")
                .and("{$project: {activite: '$_id', total : 1}}")
                .as(CountByActivity.class).iterator()));

        return stats;
    }

}
