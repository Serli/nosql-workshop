package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.utils.JestConnection;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.json.JSONArray;

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
     *
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
     *
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
            if (hits.get("total").getAsInt() >= 1) {
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
     *
     * @param string string the towns have to begin with
     * @return the town names list
     */
    public List<TownSuggest> autocompleteTown(String string) {
        try {
            // Build the request
            XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("town-suggestion")
                            .field("text", string)
                            .startObject("completion")
                                .field("field", "suggest")
                            .endObject()
                        .endObject()
                    .endObject();
            Suggest towns = new Suggest.Builder(sourceBuilder.string(), "town-suggestion").addIndex("towns").build();
            // Execute the request
            SuggestResult result = elasticClient.execute(towns);
            // Get results
            JsonArray hits = result.getJsonObject()
                    .get("town-suggestion").getAsJsonArray()
                    .get(0).getAsJsonObject()
                    .get("options").getAsJsonArray();
            List<TownSuggest> townSuggests = new ArrayList<>();
            List<Double> location = new ArrayList<>();
            hits.forEach(hit -> {
                // For each result, build a TownSuggest
                JsonObject content = hit.getAsJsonObject().get("payload").getAsJsonObject();
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
