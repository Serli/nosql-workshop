package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    public List<TownSuggest> suggest(String text) throws IOException {
        List<TownSuggest> list = new ArrayList<>();
        JestClient client = ESConnectionUtil.createClient("");

        String query = "{\n" +
                "        \"query\": {\n"+
                "           \"wildcard\": {\n"+
                "               \"townName\": {\n"+
                "                   \"value\": \"*" + text + "*\" \n" +
                "               }\n"+
                "           }\n" +
                "       }\n" +
                "}";

        Search search = (Search) new Search.Builder(query)
                .addIndex("towns")
                .addType("town")
                .build();

        JestResult result = client.execute(search);
        JsonObject object = result.getJsonObject();
        JsonArray hits = object.get("hits").getAsJsonObject().get("hits").getAsJsonArray();

        for(int i = 0; i < hits.size(); i++){
            JsonObject hit = hits.get(i).getAsJsonObject();
            JsonObject town = hit.get("_source").getAsJsonObject();
            String townName = town.get("townName").getAsString();
            JsonArray location = town.get("location").getAsJsonArray();
            Double[] ret = {location.get(0).getAsDouble(), location.get(1).getAsDouble()};
            TownSuggest suggest = new TownSuggest(townName, Arrays.asList(ret));
            list.add(suggest);
        }
        return list;
    }

    public Double[] getLocation(String townName) {
        return null;
    }
}
