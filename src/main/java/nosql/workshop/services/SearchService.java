package nosql.workshop.services;

import com.google.inject.Inject;
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
import org.jongo.MongoCollection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public SearchService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public List<Installation> search(String item){
        JestClient jestClient = ESConnectionUtil.createClient("");

        String query = "{\n" +
                " \"query\": {\n" +
                "     \"multi_match\": {\n" +
                "         \"query\": \"" + item + "\",\n" +
                "         \"fields\": [\"_all\"]\n" +
                "       }\n" +
                "   }\n" +
                "}";

        Search search = new Search.Builder(query)
                .addIndex("installations")
                .addType("installation")
                .build();

        try {
            SearchResult result = jestClient.execute(search);
            return result.getSourceAsObjectList(Installation.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
