package nosql.workshop.services;

import io.searchbox.client.JestClient;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.suggest.TownSuggest;

import java.util.ArrayList;
import java.util.List;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    private JestClient client = ESConnectionUtil.getJestClient();
	
	public List<TownSuggest> suggest(String text) {
        List<TownSuggest> list = new ArrayList<>();
        // TODO
        return list;
    }
}
