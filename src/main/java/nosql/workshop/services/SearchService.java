package nosql.workshop.services;

import com.google.inject.Inject;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import org.jongo.MongoCollection;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

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
