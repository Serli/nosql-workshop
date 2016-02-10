package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;

import java.io.IOException;
import java.util.List;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    public List<Installation> get(String queryString) throws IOException {
        String query =
                "{" +
                "    \"query\": {" +
                "        \"filtered\" : {" +
                "            \"query\" : {" +
                "                \"query_string\" : {" +
                "                    \"query\" : \"" + queryString.toLowerCase() + "\"" +
                "                }" +
                "            }" +
                "        }" +
                "    }" +
                "}";

        Search search = new Search.Builder(query).addIndex("installations").addType("installation").build();

        JestClient client = ESConnectionUtil.createClient("");
        JestResult searchResult = client.execute(search);
        client.shutdownClient();

        return searchResult.getSourceAsObjectList(Installation.class);
    }

    public Double[] getLocation(String townName) throws IOException {
        String query =
                "{" +
                "        \"query\": {"+
                "           \"match\": {"+
                "               \"townName\": \"" + townName + "\"" +
                "           }" +
                "       }" +
                "}";

        JestClient client = ESConnectionUtil.createClient("");
        Search search = new Search.Builder(query).addIndex("towns").addType("town").build();
        JestResult result = client.execute(search);
        client.shutdownClient();

        JsonArray hits = result.getJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray();

        if(hits.size() > 0){
            JsonObject town = hits.get(0).getAsJsonObject().get("_source").getAsJsonObject();
            JsonArray location = town.get("location").getAsJsonArray();
            return new Double[] { location.get(0).getAsDouble(), location.get(1).getAsDouble() };
        }

        return null;
    }
}
