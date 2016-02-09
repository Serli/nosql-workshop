package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import net.codestory.http.Context;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

   public List<TownSuggest> suggest(String text) {
       List<TownSuggest> list = new ArrayList<TownSuggest>();
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

       JestResult result = null;
       try {
           result = client.execute(search);
       } catch (IOException e) {
           e.printStackTrace();
       }

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

    public List<Installation> search(Context context) {
        JestClient client = ESConnectionUtil.createClient("");

      /*  String query = "{\"query\": {\n" +
                "            \"multi_match\": {\n" +
                "                \"query\": \"Carquefou\",\n" +
                "                        \"fields\": [\"_all\"]\n" +
                "            }\n" +
                "        }\n" +
                "    }'}";

        Search search = (Search) new Search.Builder(query)
                .addIndex("installations")
                .addType("installation")
                .build();

        JestResult result = null;
        try {
            result = client.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        JsonObject object = result.getJsonObject();
        JsonArray hits = object.get("hits").getAsJsonObject().get("hits").getAsJsonArray();
        if(hits.size() > 0){
            String firstHit = hits.get(0).getAsString();
            return null;
        }
        else {
            return null;
        }*/
        return null;
    }

    public Double[] getLocation(String townName) {
        JestClient client = ESConnectionUtil.createClient("");
        String query = "{\n" +
                " \"query\": {\n" +
                "     \"match\": {\n" +
                "         \"townName\": \"" + townName + "\"\n" +
                "       }\n" +
                "   }\n" +
                "}";

        Search search = new Search.Builder(query).addIndex("towns").addType("town").build();

        try {
            SearchResult searchResult = client.execute(search);
            List<SearchResult.Hit<TownSuggest, Void>> hits = searchResult.getHits(TownSuggest.class);
            if (!hits.isEmpty()) {
                SearchResult.Hit<TownSuggest, Void> townHit = hits.get(0);
                return townHit.source.getLocation();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
