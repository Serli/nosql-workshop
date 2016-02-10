package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import net.codestory.http.Context;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

   public List<TownSuggest> suggest(String text) {
       JestClient client = ESConnectionUtil.createClient("");
       String query = "{\n" +
               "        \"query\": {\n"+
               "           \"wildcard\": {\n"+
               "               \"townName\": {\n"+
               "                   \"value\": \"" + text + "*\" \n" +
               "               }\n"+
               "           }\n" +
               "       }\n" +
               "}";

       Search search = new Search.Builder(query).addIndex("towns").addType("town").build();
       List<TownSuggest> result = null;

       try {
           SearchResult searchResult = client.execute(search);
           if (searchResult.isSucceeded()) {
               List<SearchResult.Hit<TownSuggest, Void>> hits = searchResult.getHits(TownSuggest.class);
               result = hits.stream().map(townHit -> townHit.source).collect(Collectors.toList());
           }
       } catch (IOException e) {
           throw new RuntimeException(e);
       }
       return result;
   }

    public List<Installation> search(Context context) {
        String q = context.query().get("query");

        JestClient client = ESConnectionUtil.createClient("");
        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"multi_match\": {\n" +
                "           \"query\": \""+q+"\",\n" +
                "           \"fields\": [\"_all\"]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        Search search = new Search.Builder(query).addIndex("installations").addType("installation").build();
        List<Installation> listInstallation = null;

        try {
            SearchResult searchResult = client.execute(search);
            if (searchResult.isSucceeded()) {
                List<SearchResult.Hit<Installation, Void>> hits = searchResult.getHits(Installation.class);
                if (!hits.isEmpty()) {
                    listInstallation = hits.stream().map(sr -> sr.source).collect(Collectors.toList());
                    //assez horrible, mais nous manquons de temps
                    listInstallation.stream().forEach(ins -> {
                        if(ins.getEquipements() == null){
                            ins.setEquipements(new ArrayList<Equipement>());
                        }
                        if(ins.getAdresse().getVoie() == null){
                            ins.getAdresse().setVoie("");
                        }
                        if(ins.getAdresse().getNumero() == null){
                            ins.getAdresse().setNumero("");
                        }
                    });
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return listInstallation;
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
        Double[] result = null;

        try {
            SearchResult searchResult = client.execute(search);
            List<SearchResult.Hit<TownSuggest, Void>> hits = searchResult.getHits(TownSuggest.class);
            if (!hits.isEmpty()) {
                SearchResult.Hit<TownSuggest, Void> townHit = hits.get(0);
                result = townHit.source.getLocation();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
