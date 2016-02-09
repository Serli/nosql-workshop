package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.IOException;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    public Double[] getLocation(String townName) throws IOException{
        JestClient client = ESConnectionUtil.createClient("");

        String query = "{\n" +
                "        \"query\": {\n"+
                "           \"match\": {\n"+
                "               \"townName\": \"" + townName + "\" \n" +
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
        if(hits.size() > 0){
            JsonObject firstHit = hits.get(0).getAsJsonObject();
            JsonObject town = firstHit.get("_source").getAsJsonObject();
            JsonArray location = town.get("location").getAsJsonArray();
            Double[] ret = {location.get(0).getAsDouble(), location.get(1).getAsDouble()};
            return ret;
        }
        else {
            return null;
        }
    }
}
