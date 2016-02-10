package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    private JestClient client = ESConnectionUtil.getJestClient();
	
	public List<TownSuggest> suggest(String text) {
        List<TownSuggest> list = new ArrayList<>();
        // TODO https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters-completion.html
        return list;
    }

    public List<Installation> list() {
        List<Installation> list = null;

        String query = "{ \n" +
                "  \"query\" : {\n" +
                "    \"match_all\": {} \n" +
                "  }\n" +
                "}";

        Search search = new Search.Builder(query)
                // multiple index or types can be added.
                .addIndex("towns")
                .build();

        // TODO Doesn't work at all
        try {
            SearchResult result = client.execute(search);
            list = result.getSourceAsObjectList(Installation.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }
}
