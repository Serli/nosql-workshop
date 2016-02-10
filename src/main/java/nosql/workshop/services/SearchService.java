package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
@Singleton
public class SearchService {

    @Inject
    public SearchService() throws UnknownHostException {

    }

    public List<TownSuggest> suggest(String text) throws IOException {

        JestClient client = ESConnectionUtil.createClient("MONGOLAB_URI");

        String query2 = "{\n"
                + "    \"query\": {\n"
                + "        \"filtered\" : {\n"
                + "            \"query\" : {\n"
                + "                \"query_string\" : {\n"
                + "                    \"query\" : \"java\"\n"
                + "                }\n"
                + "            }"
                + "        }\n"
                + "    }\n"
                + "}";

        String query = "{\"suggest\" : {\n"
                + "         \"town-suggest\" : {\n"
                + "             \"text\" : \"n\",\n"
                + "             \"term\" : {\n"
                + "                 \"field\" : \"townName\"\n"
                + "             }\n"
                + "         }"
                + "     }}";


       //Suggest suggest = new Suggest.Builder(query, "suggest").addIndex("towns").build();
        Search.Builder searchBuilder = new Search.Builder(query).addIndex("towns");


        SearchResult result = client.execute(searchBuilder.build());
        System.out.println("suggestResult");

        List<SearchResult.Hit<TownSuggest, Void>> hits = result.getHits(TownSuggest.class);
        List<TownSuggest> towns = new ArrayList<TownSuggest>();
        if (!hits.isEmpty()) {
            for (SearchResult.Hit<TownSuggest, Void> hit : hits) {
                towns.add(hit.source);
            }
        }
        return towns;
    }

    public Double[] getLocation(String townName) throws IOException {

        JestClient client = ESConnectionUtil.createClient("MONGOLAB_URI");

        String query = "{\"query\": {\n"
            + "     \"match\": {\n"
            + "         \"townName\": \"" + townName + "\"\n"
            + "     }\n"
            + "}}";


        Search.Builder searchBuilder = new Search.Builder(query).addIndex("towns").addType("town");

        SearchResult result = client.execute(searchBuilder.build());
        List<SearchResult.Hit<TownSuggest, Void>> hits = result.getHits(TownSuggest.class);
        if (!hits.isEmpty()) {
            SearchResult.Hit<TownSuggest, Void> townHit = hits.get(0);
            return townHit.source.getLocation();
        }
        else {
            return null;
        }
    }


    public List<Installation> search(String query) throws IOException {
        List<Installation> installations = new ArrayList<Installation>();

        JestClient client = ESConnectionUtil.createClient("");


        String querySearch2 = "{\"query\": {\n"
                + "     \"match\": {\n"
                + "         \"nom\": \"" + query + "\"\n"
                + "     }\n"
                + "}}";


        String querySearch = "{"
                + " \"query\": {\n"
                + "     \"multi_match\": {\n"
                + "         \"query\": \"" + query + "\",\n"
                + "         \"fields\": [\n"
                + "             \"_all\"\n"
                + "         ]\n"
                + "     }\n"
                + " }\n"
                + "}";

        Search.Builder searchBuilder = new Search.Builder(querySearch).addIndex("installations").addType("installation");
        System.out.println(querySearch);

        SearchResult result = client.execute(searchBuilder.build());

        List<SearchResult.Hit<Installation, Void>> hits = result.getHits(Installation.class);
        Stream<Installation> installationStream = hits.stream().map(installation -> installation.source);
        return Lists.newArrayList(installationStream.iterator());
        /** if (!hits.isEmpty()) {
         for (SearchResult.Hit<Installation, Void> hit : hits) {
         installations.add(hit.source);
         }
         }
         return installations;**/
    }
}
