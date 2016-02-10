package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;

import java.io.IOException;
import java.util.List;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {
    public List<Installation> get(String queryString) throws IOException {
        JestClient jest = ESConnectionUtil.createClient("");

        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"filtered\" : {\n" +
                "            \"query\" : {\n" +
                "                \"query_string\" : {\n" +
                "                    \"query\" : \"" + queryString.toLowerCase() + "\"\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        Search search = new Search.Builder(query).addIndex("installations").addType("installation").build();

        JestResult searchResult = jest.execute(search);

        return searchResult.getSourceAsObjectList(Installation.class);
    }
}
