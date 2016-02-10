package nosql.workshop.services;

import com.google.common.collect.Iterators;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;
import nosql.workshop.model.suggest.TownSuggest;
import org.jongo.FindOne;
import org.jongo.MongoCollection;
import net.codestory.http.Context;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
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
        return installations.findOne("{_id : '" + Id + "'}").as(Installation.class);
    }

    public List<Installation> getAll() {
        return Lists.newArrayList(installations.find().as(Installation.class).iterator());
    }

    public InstallationsStats getStats() {
        InstallationsStats stats = new InstallationsStats();
        stats.setTotalCount(installations.count(""));
        stats.setCountByActivity(Lists.newArrayList(installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$unwind: \"$equipements.activites\"}")
                .and("{$group: {_id: \"$equipements.activites\", total:{$sum : 1}}}")
                .and("{$project: {activite: \"$_id\", total : 1}}")
                .as(CountByActivity.class).iterator()));
        stats.setAverageEquipmentsPerInstallation(installations.aggregate("{$group: {_id: null, average : { $avg : { $size : \"$equipements\"}}}}")
                .as(Average.class).next().getAverage());
        IdForMaxEquipements idMax = installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$group: {_id: \"$_id\", total:{$sum : 1}}}")
                .and("{$sort: {total : -1}}")
                .and("{$limit: 1}")
                .and("{$project : {id: \"$_id\", _id: 0}}")
                .as(IdForMaxEquipements.class)
                .next();
        stats.setInstallationWithMaxEquipments(getById(idMax.id));
        return stats;
    }

    public List<Installation> geoSearch(Double latitude, Double longitude, Integer dist) {
        installations.ensureIndex("{ location : '2dsphere' } ");
        return Lists.newArrayList(installations.find("{location : { $near : { $geometry : { type : \"Point\", coordinates : [ " + latitude + ", " + longitude + " ]}, $maxDistance : " + dist + "}}}").as(Installation.class).iterator());

    }

    public List<Installation> search(Context context) throws IOException {

        String query = "{\n" +
                "        \"query\": {\n"+
                "           \"multi_match\": {\n"+
                "               \"query\": \"" + context.query().get("query") + "\",\n" +
                "                    \"fields\": [\"_all\"] \n"+
                "               }\n"+
                "           }\n" +
                "       }\n" +
                "}";
        Search search = (Search) new Search.Builder(query)
                .addIndex("installations")
                .addType("installation")
                .build();

        JestResult result = ESConnectionUtil.createClient("").execute(search);


        List<Installation> installations = new ArrayList<>();

       installations = result.getSourceAsObjectList(Installation.class);
        if(!installations.isEmpty() && installations != null){
            JsonObject object = result.getJsonObject();
            JsonArray hits = object.get("hits").getAsJsonObject().get("hits").getAsJsonArray();
            JsonObject hit = hits.get(0).getAsJsonObject();
            JsonObject jsonObjectInstallation = hit.getAsJsonObject("_source");
            JsonObject jsonObjectLocation = jsonObjectInstallation.getAsJsonObject("location");

            JsonArray jsonArrayCoordinates = jsonObjectLocation.getAsJsonArray("coordinates");

           double[] coordinates = {jsonArrayCoordinates.get(0).getAsDouble(), jsonArrayCoordinates.get(1).getAsDouble()};
            installations.get(0).getLocation().setCoordinates(coordinates);
        }
        return installations;
    }


    public static class IdForMaxEquipements {
        public String id;
    }
}