package nosql.workshop.resources;

import com.google.inject.Inject;
import io.searchbox.client.JestClient;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.MongoDB;
import nosql.workshop.services.SearchService;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

    private final SearchService searchService;
    private final InstallationService installationService;

    @Inject
    public TownResource(SearchService searchService, InstallationService installationService) {

            this.searchService = searchService;
            this.installationService = installationService;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {

        return searchService.suggest(text);
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName){

        return searchService.getLocation(townName);
    }
}
