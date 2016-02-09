package nosql.workshop.services;

import com.google.common.collect.Iterators;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.FindOne;
import org.jongo.MongoCollection;
import net.codestory.http.Context;

import java.net.UnknownHostException;
import java.util.HashMap;
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

    public Installation getById(String Id) {
        return installations.findOne("{_id : '"+Id+"'}").as(Installation.class);
    }

    public List<Installation> getAll() {
        return Lists.newArrayList(installations.find().as(Installation.class).iterator());
    }

    public InstallationsStats getStats(){
        InstallationsStats stats = new InstallationsStats();
        stats.setTotalCount(installations.count(""));
        stats.setCountByActivity(Lists.newArrayList(installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$unwind: \"$equipements.activites\"}")
                .and("{$group: {_id: \"$equipements.activites\", total:{$sum : 1}}}")
                .and("{$project: {activite: \"$_id\", total : 1}}")
                .as(CountByActivity.class).iterator()));
        stats.setAverageEquipmentsPerInstallation(installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$group: {_id: \"$nom\", sum:{$sum : 1}}}")
                .and("{$group: {_id: 0, average:{$avg : \"$sum\"}}}")
                .as(Average.class).next().getAverage());
       // stats.setAverageEquipmentsPerInstallation(installations.aggregate(" [{$unwind : \"$equipements\"},{ $group : { _id : \"$_id\", len : { $sum : 1 } } },{ $sort : { len : -1 } },{ $limit :1 } ]")
       //         .as(Installation));
        return stats;
    }

    public  List<Installation> geoSearch(Double latitude, Double longitude, Integer dist) {
        installations.ensureIndex("{ location : '2dsphere' } " );
        return Lists.newArrayList(installations.find("{location : { $near : { $geometry : { type : \"Point\", coordinates : [ "+latitude+", "+longitude+" ]}, $maxDistance : "+dist+"}}}").as(Installation.class).iterator());

    }

}
