package nosql.workshop.services;

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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

    public List<Installation> search(String query) {
        return new ArrayList<>();
    }

    public List<Installation> geosearch(Double lat, Double lng, Integer distance) {
        return new ArrayList<>();
    }

    public List<TownSuggest> suggest(String text) {
        return new ArrayList<>();
    }
}
