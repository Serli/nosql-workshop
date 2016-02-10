package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.BasicDBObject;
import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;
import org.bson.types.ObjectId;
import org.jongo.MongoCollection;
import org.jongo.marshall.jackson.oid.MongoId;

import java.net.UnknownHostException;
import java.util.ArrayList;
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
        return installations.findOne().as(Installation.class);
    }

    public Installation get(String numero) {
        return installations.findOne("{_id : '" + numero + "'}").as(Installation.class);
    }

    public List<Installation> list() {
        return Lists.newArrayList(installations.find("").as(Installation.class).iterator());
    }

    public List<Installation> geoSearch(Context context){
        String latitude = context.query().get("lat");
        String longitude = context.query().get("lng");
        String distance = context.query().get("distance");
        installations.ensureIndex("{ location : '2dsphere' } " );
        return Lists.newArrayList(installations.find(
                "{location : { $near : { $geometry : { type : \"Point\", coordinates : [ "+longitude+", "+latitude+" ]}, $maxDistance : "+distance+"}}}"
        ).as(Installation.class).iterator());
    }

    public InstallationsStats stats() {
        InstallationsStats installationsStats = new InstallationsStats();
        installationsStats.setCountByActivity(
                Lists.newArrayList(installations
                .aggregate("{$unwind: \"$equipements\"}")
                .and("{$unwind: \"$equipements.activites\"}")
                .and("{$group: {_id: \"$equipements.activites\", total:{$sum : 1}}}")
                .and("{$project: {activite: \"$_id\", total : 1}}")
                .as(CountByActivity.class).iterator()));
        installationsStats.setInstallationWithMaxEquipments(installations.findOne("{ _id : # }", installations
                .aggregate("{ $project : { nbEquip : { $size: '$equipements' } } }")
                .and("{ $sort : { nbEquip : -1 } }")
                .and("{ $limit : 1 }")
                .as(Installation.class).next().get_id()).as(Installation.class));
        installationsStats.setAverageEquipmentsPerInstallation(installations
                .aggregate("{$unwind : '$equipements'}")
                .and("{ $group : { _id : '$_id', total : { $sum : 1 } } }")
                .and("{ $group : { _id : 0, average : { $avg : '$total' } } }")
                .and("{ $project : { _id : 0, average : 1 } }")
                .as(Average.class).next().getAverage());
        installationsStats.setTotalCount(installations.count());
        return installationsStats;
    }
}