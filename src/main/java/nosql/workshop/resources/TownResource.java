package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.SearchService;

import java.io.IOException;
import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

    private final SearchService searchService;


    @Inject
    public TownResource(SearchService service) {
        this.searchService = service;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {

        try {
            return searchService.suggest(text);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName){
        try {
            return searchService.getLocation(townName);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
