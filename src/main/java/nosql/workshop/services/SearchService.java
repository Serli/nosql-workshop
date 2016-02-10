package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    public List<Installation> search(String _query) {
        JestClient client = ESConnectionUtil.createClient("");
        String query = "{\n" +
                " \"query\": {\n" +
                "     \"multi_match\": {\n" +
                "         \"query\": \"" + _query + "\",\n" +
                "         \"fields\": [\"_all\"]\n" +
                "       }\n" +
                "   }\n" +
                "}";

        Search search = new Search.Builder(query).addIndex("installations").addType("installation").build();

        try {
            SearchResult searchResult = client.execute(search);
            if (searchResult.isSucceeded()) {
                return searchResult.getHits(Installation.class)
                        .stream().map(installation -> installation.source).collect(Collectors.toList());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public List<TownSuggest> suggest(String text) {
        return suggestWithSearch(text);
        //return suggestWithSuggestAPI(text);
    }

    private List<TownSuggest> suggestWithSuggestAPI(String text) {
        JestClient client = ESConnectionUtil.createClient("");
        String suggestQuery =
                "{\n"+
                        "    \"suggest\" : {\n" +
                        "        \"text\" : \"" + text + "\", \n" +
                        "        \"term\" : {\n" +
                        "            \"field\" : \"townName\" \n" +
                        "        }\n" +
                        "    }\n" +
                        "}";

        Suggest suggest = new Suggest.Builder(suggestQuery, "suggest").addIndex("towns").build();

        try {
            SuggestResult suggestResult = client.execute(suggest);
            if (suggestResult.isSucceeded()) {
                //suggestResult.getSuggests().forEach(System.out::println);
                return suggestResult.getSuggests()
                        .stream().map(str -> new TownSuggest(str, Arrays.asList(new Double[]{})))
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private List<TownSuggest> suggestWithSearch(String text) {
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

        try {
            SearchResult searchResult = client.execute(search);
            if (searchResult.isSucceeded()) {
                List<SearchResult.Hit<TownSuggest, Void>> hits = searchResult.getHits(TownSuggest.class);
                return hits.stream().map(townHit -> townHit.source).collect(Collectors.toList());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            if (searchResult.isSucceeded()) {
                List<SearchResult.Hit<TownSuggest, Void>> hits = searchResult.getHits(TownSuggest.class);
                if (!hits.isEmpty()) {
                    return hits.get(0).source.getLocation();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
