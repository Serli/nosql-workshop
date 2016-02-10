package nosql.workshop.resources;

import com.google.inject.Inject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import net.codestory.http.annotations.Get;
import net.codestory.http.errors.NotFoundException;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.SearchService;

import java.io.IOException;
import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

    private SearchService service;

    @Inject
    public TownResource(SearchService service ) {
        this.service = service;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {
        try {
            return service.getSuggestion(text);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName) {
        Double[] result = new Double[0];
        try {
            result = service.getLocation(townName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return NotFoundException.notFoundIfNull(result);
    }
}
