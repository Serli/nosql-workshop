package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.SearchService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

    private final SearchService searchService;

    @Inject
    public TownResource(SearchService searchService) {
        this.searchService = searchService;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {
        List<TownSuggest> towns = new ArrayList<TownSuggest>();
        try {
            towns = searchService.suggest(text);
        } catch (IOException e) {
            e.getStackTrace();
        }
        return towns;
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName){
        Double[] location = new Double[2];
        try {
            location = searchService.getLocation(townName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return location;
    }
}
