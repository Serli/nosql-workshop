package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.BasicDBObject;
import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
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
        InstallationsStats stats = new InstallationsStats();
        /*stats.setAverageEquipmentsPerInstallation(installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$group: {numero: \"$nom\", sum:{$sum : 1}}}")
                .and("{$group: {numero: 0, avg:{$avg : 1}}}").as(Double.class));*/
        stats.setCountByActivity(Lists.newArrayList(installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$unwind: \"$equipements.activites\"}")
                .and("{$group: {numero: \"$equipements.activites\", total:{$sum : 1}}}")
                .and("{$project: {activite: \"$numero\", total : 1}}")
                .as(CountByActivity.class).iterator()));
        return stats;
    }
}