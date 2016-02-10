package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.suggest.TownSuggest;

import java.io.IOException;
import java.util.List;

/**
 * Created by ahuberty on 10/02/2016.
 */
public class TownService {

    public Double[] getLocation(String townName) {
        JestClient jestClient = ESConnectionUtil.createClient("");
        String query = "{\n" +
                " \"query\": {\n" +
                "     \"match\": {\n" +
                "         \"townName\": \"" + townName + "\"\n" +
                "       }\n" +
                "   }\n" +
                "}";

        Search search = new Search.Builder(query)
                .addIndex("towns")
                .addType("town")
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            return result.getSourceAsObject(TownSuggest.class).getLocation();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public List<TownSuggest> suggest(String text) {
        JestClient jestClient = ESConnectionUtil.createClient("");
        String suggest = "{\n"+
                        "   \"suggest\" : {\n" +
                        "       \"suggest-town\" : {"  +
                        "           \"text\" : \"" + text + "\", \n" +
                        "           \"term\" : {\n" +
                        "               \"field\" : \"townName\" \n" +
                        "           }\n" +
                        "       }\n" +
                        "   }\n" +
                        "}";

        Search search = new Search.Builder(suggest)
                .addIndex("towns")
                .addType("town")
                .build();

        try {
            SearchResult result = jestClient.execute(search);
            return result.getSourceAsObjectList(TownSuggest.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
