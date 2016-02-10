package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
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

    private String ES_INDEX = "nosql-workshop";
    private String ES_TYPE = "towns";

    public List<TownSuggest> suggest(String text) throws IOException {
        List<TownSuggest> list = new ArrayList<>();
        JestClient client = ESConnectionUtil.createClient("");

        String query = "{\n" +
                "        \"query\": {\n"+
                "           \"wildcard\": {\n"+
                "               \"nameSuggest\": {\n"+
                "                   \"value\": \"*" + text + "*\" \n" +
                "               }\n"+
                "           }\n" +
                "       }\n" +
                "}";

        Search search = new Search.Builder(query)
                .addIndex(ES_INDEX)
                .addType(ES_TYPE)
                .build();

        JestResult result = client.execute(search);

        JsonObject object = result.getJsonObject();
        JsonArray hits = object.get("hits").getAsJsonObject().get("hits").getAsJsonArray();

        for(int i = 0; i < hits.size(); i++){
            JsonObject hit = hits.get(i).getAsJsonObject();
            JsonObject town = hit.get("_source").getAsJsonObject();
            String townName = town.get("nameSuggest").getAsString();
            JsonArray location = town.get("townLocation").getAsJsonArray();
            Double[] ret = {location.get(0).getAsDouble(), location.get(1).getAsDouble()};
            TownSuggest suggest = new TownSuggest(townName, Arrays.asList(ret));
            list.add(suggest);
        }
        return list;
    }


    public Double[] getLocation(String townName) throws IOException {
        JestClient client = ESConnectionUtil.createClient("");

        String query = "{\n" +
                "        \"query\": {\n"+
                "           \"wildcard\": {\n"+
                "               \"name\": \""+ townName.toLowerCase() + "\"" +
                "           }\n" +
                "       }\n" +
                "}";

        Search search = new Search.Builder(query)
                .addIndex(ES_INDEX)
                .addType(ES_TYPE)
                .build();

        JestResult result = client.execute(search);

        JsonObject object = result.getJsonObject();
        JsonObject jsonObject = object.get("hits").getAsJsonObject().get("hits").getAsJsonArray().get(0).getAsJsonObject();

        JsonObject town = jsonObject.get("_source").getAsJsonObject();
        JsonArray location = town.get("townLocation").getAsJsonArray();
        Double[] townLocation = {location.get(0).getAsDouble(), location.get(1).getAsDouble()};

        return townLocation;
    }
}
