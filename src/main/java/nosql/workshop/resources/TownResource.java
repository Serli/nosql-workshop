package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.Town;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.SearchService;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

    private final SearchService searchService;

    @Inject
    public TownResource(SearchService search) {
        searchService = search;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {
        return new ArrayList<TownSuggest>(); // ...
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName) throws IOException{
        return searchService.getLocation(townName);
    }
}
