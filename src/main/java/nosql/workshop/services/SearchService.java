package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.resources.TownResource;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    public List<Installation> search(String query) {
        return new ArrayList<>();
    }

    public List<TownSuggest> suggest(String text) {
        return null;
    }

    public Double[] getLocation(String townName) {

        /*JestClient client = ESConnectionUtil.createClient("");

        String query = "{\n" +
                        " \"query\": {\n" +
                        "     \"match\": {\n" +
                        "         \"townName\": \"" + townName + "\"\n" +
                        "       }\n" +
                        "   }\n" +
                        "}";

        Search search = new Search.Builder(query).addIndex("towns").addType("town").build();*/



       /* try {
            SearchResult searchResult = client.execute(search);
            List<SearchResult.Hit<TownSuggest, Void>> hits = searchResult.getHits(TownSuggest.class);
            if (!hits.isEmpty()) {
                SearchResult.Hit<TownSuggest, Void> townHit = hits.get(0);
                return townHit.source.getLocation();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
        return null;

    }

}
