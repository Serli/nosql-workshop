package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.searchbox.client.JestClient;
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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
@Singleton
public class SearchService {

    private final JestClient elasticClient;

    @Inject
    public SearchService() {
        this.elasticClient = ESConnectionUtil.createClient();
    }

    public List<Installation> list(String search) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(search, "nom", "adresse.*", "equipements.*"));
        Search research = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("installations")
                .addType("installation")
                .build();
        SearchResult result = null;
        try {
            result = elasticClient.execute(research);
            return result.getSourceAsObjectList(Installation.class);
        } catch (IOException e) {
            return null;
        }
    }

}
