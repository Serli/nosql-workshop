package nosql.workshop.services;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;
import net.codestory.http.Context;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import org.elasticsearch.common.collect.Lists;
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
import java.util.stream.Stream;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    public List<Installation> search(Context context) throws IOException {

        String q = context.get("query");

        String query = "{\"query\": {\"multi_match\": {\"query\": \""+q+"\", \"fields\": [\"_all\"] }}}";

        SearchResult searchResult = this.executeSearch(query, "installation");
        List<SearchResult.Hit<Installation, Void>> hits = searchResult.getHits(Installation.class);
        Stream<Installation> installationStream = hits.stream().map(installation -> installation.source);
        return Lists.newArrayList(installationStream.iterator());

    }

    public List<TownSuggest> suggest(String text) throws IOException {

        List<TownSuggest> list = new ArrayList<>();

        String query = "{\"query\": {\"wildcard\": {\"townName\": {\"value\": \""+text +"*\"}}}}";

        SearchResult searchResult = this.executeSearch(query, "town");
        List<SearchResult.Hit<TownSuggest, Void>> hits = searchResult.getHits(TownSuggest.class);
        Stream<TownSuggest> townSuggestStream = hits.stream().map(townHit -> townHit.source);
        return Lists.newArrayList(townSuggestStream.iterator());

    }

    public Double[] getLocation(String townName) throws IOException {

        String query = "{\"query\": {\"match\": {\"townName\": \""+townName+"\"}}}";

        SearchResult searchResult = this.executeSearch(query, "town");
        List<SearchResult.Hit<TownSuggest, Void>> hits = searchResult.getHits(TownSuggest.class);
        if (!hits.isEmpty()) {
            SearchResult.Hit<TownSuggest, Void> townHit = hits.get(0);
            return townHit.source.getLocation();
        }
        else {
            return null;
        }

    }

    private SearchResult executeSearch(String query, String type) throws IOException {

        JestClient client = ESConnectionUtil.createClient("");

        Search search = new Search.Builder(query)
                .addIndex(type + "s")
                .addType(type)
                .build();

        return client.execute(search);

    }
}
