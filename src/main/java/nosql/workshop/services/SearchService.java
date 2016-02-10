package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;
import nosql.workshop.batch.mongodb.CsvToMongoDb;
import nosql.workshop.connection.ESConnection;
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
public class SearchService {

    private static JestClient CLIENT = new ESConnection("localhost","9200").getClient();



    public List<TownSuggest> suggestions(String text) {

        try {

           List<String> suggests =   CLIENT.execute(new Suggest.Builder(multiQuery(text),text).addType(CsvToMongoDb.VILLES).addIndex(CsvToMongoDb.VILLES).build()).getSuggests();
            for(int i=0;i<suggests.size();i++){
                String suggest = suggests.get(i);
                System.out.println(suggest);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }



    public static String multiQuery(String query){

        return String.join("\n",

                "{",
                        "*query*: {",
            "*multi_match*: {",
                "*query*: *"+query+"*,",
                        "*fields*: [*_all*]",
            "}",
        "}",
        "}"    ).replace('*','"');
    }


}
