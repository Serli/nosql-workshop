package nosql.workshop.resources;

import com.google.inject.Inject;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.SearchService;

import java.io.IOException;
import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

	private static SearchService searchService = new SearchService();
    @Inject
    public TownResource() {
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) throws IOException {
        return searchService.suggest(text);
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName) throws IOException{
        return searchService.getLocation(townName);
    }
}
