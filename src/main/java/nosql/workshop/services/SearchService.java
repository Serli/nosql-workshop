package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.utils.JestConnection;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
@Singleton
public class SearchService {

    private final JestClient elasticClient;

    @Inject
    public SearchService() {
        this.elasticClient = JestConnection.createClient();
    }

    /**
     * Gets the list of the installations matching a given string.
     * @param search string to search
     * @return list of installations
     */
    public List<Installation> list(String search) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(search, "nom", "adresse.*", "equipements.*"));
        Search research = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("installations")
                .addType("installation")
                .build();
        try {
            SearchResult result = elasticClient.execute(research);
            List<Installation> installations = new ArrayList<>();
            result.getHits(Installation.class).iterator().forEachRemaining(hit -> installations.add(hit.source));
            return installations;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Gets the location of a town.
     * @param town town to get the location
     * @return location of the town
     */
    public Double[] locationOf(String town) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("townname", town));
        Search research = new Search.Builder(searchSourceBuilder.toString()).addIndex("towns").addType("town").build();
        try {
            SearchResult result = elasticClient.execute(research);
            JsonObject hits = result.getJsonObject().get("hits").getAsJsonObject();
            if (hits.get("total").getAsInt() == 1) {
                JsonObject hit = hits.get("hits").getAsJsonArray().get(0).getAsJsonObject().get("_source").getAsJsonObject();
                Double[] location = {hit.get("x").getAsDouble(), hit.get("y").getAsDouble()};
                return location;
            } else {
                return null;
            }
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Gets the town names beginning with a given string.
     * @param string string the towns have to begin with
     * @return the town names list
     */
    public List<TownSuggest> autocompleteTown(String string) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.prefixQuery("townname", string.toLowerCase()));
        Search research = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("towns")
                .addType("town")
                .build();
        try {
            SearchResult result = elasticClient.execute(research);
            System.out.println(result.getJsonObject().toString());
            JsonArray hits = result.getJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray();
            List<TownSuggest> townSuggests = new ArrayList<>();
            List<Double> location = new ArrayList<>();
            hits.forEach(hit -> {
                JsonObject content = hit.getAsJsonObject().get("_source").getAsJsonObject();
                String name = content.get("townname").getAsString();
                location.add(content.get("x").getAsDouble());
                location.add(content.get("y").getAsDouble());
                townSuggests.add(new TownSuggest(name, location));
                location.clear();
            });
            return townSuggests;
        } catch (IOException e) {
            return null;
        }
    }

}
