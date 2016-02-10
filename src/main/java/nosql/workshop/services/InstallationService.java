package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;
import nosql.workshop.model.suggest.TownSuggest;
import org.jongo.MongoCollection;

import net.codestory.http.Context;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

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
         installations.ensureIndex("{ location : '2dsphere' } ");
         return Lists.newArrayList(installations.find("{'location' : { $near : { $geometry : { type : 'Point', coordinates: ["+lng+", "+lat+"]}, $maxDistance : "+distance+"}}}")
                .as(Installation.class).iterator());
    }

    public InstallationsStats stats() {
        InstallationsStats stats = new InstallationsStats();

        long total = installations.count();
        System.out.println("Debug total : " + total);
        stats.setTotalCount(total);

        Installation installation = installations.aggregate("{$project: {equip: {$size: '$equipements'}}}")
                .and("{$sort: {equip: -1}}")
                .and("{$limit: 1}")
                .as(Installation.class).next();
        Installation maxInstall = installations.findOne("{_id: #}", installation.get_id())
                .as(Installation.class);
        System.out.println("Debug installation : " + installation.get_id());
        stats.setInstallationWithMaxEquipments(maxInstall);

        double moyenne = installations.aggregate("{$unwind: '$equipements'}")
                .and("{$group: {_id : '$_id', total: {$sum : 1}}}")
                .and("{$group: {_id : 0, average: {$avg : '$total'}}}")
                .and("{$project: {_id : 0, average: 1}}")
                .as(Average.class).next().getAverage();
        System.out.println("Debug moyenne : " + moyenne);
        stats.setAverageEquipmentsPerInstallation(moyenne);

        ArrayList<CountByActivity> listCount = Lists.newArrayList(installations.aggregate("{$unwind:'$equipements'}")
                .and("{$unwind: '$equipements.activites'}")
                .and("{$group: {_id: '$equipements.activites', total:{$sum : 1}}}")
                .and("{$project: {activite: '$_id', total : 1}}")
                .and("{$sort: {total: -1}}")
                .as(CountByActivity.class).iterator());
        System.out.println("Debug activité : " + listCount.get(0).getActivite());
        System.out.println("Debug nb instal : " + listCount.get(0).getTotal());
        stats.setCountByActivity(listCount);

        return stats;
    }

    public List<Installation> search(String query) throws IOException {
        List<Installation> installations = new ArrayList<Installation>();

        JestClient client = ESConnectionUtil.createClient("");


        String querySearch2 = "{\"query\": {\n"
                + "     \"match\": {\n"
                + "         \"nom\": \"" + query + "\"\n"
                + "     }\n"
                + "}}";


        String querySearch3 = "{"
            + " \"query\": {\n"
            + "     \"multi_match\": {\n"
            + "         \"query\": \"" + query + "\",\n"
            + "         \"fields\": [\n"
            + "             \"_all\"\n"
            + "         ]\n"
            + "     }\n"
            + " }\n"
            + "}";

        String querySearch = "{\"query\": {\"multi_match\": {\"query\": \""+query+"\", \"fields\": [\"_all\"] }}}";
        Search.Builder searchBuilder = new Search.Builder(querySearch).addIndex("installations").addType("installation");
        System.out.println(querySearch);

        SearchResult result = client.execute(searchBuilder.build());

        List<SearchResult.Hit<Installation, Void>> hits = result.getHits(Installation.class);
        Stream<Installation> installationStream = hits.stream().map(installation -> installation.source);
        return Lists.newArrayList(installationStream.iterator());
       /** if (!hits.isEmpty()) {
            for (SearchResult.Hit<Installation, Void> hit : hits) {
                installations.add(hit.source);
            }
        }
        return installations;**/
    }
}
