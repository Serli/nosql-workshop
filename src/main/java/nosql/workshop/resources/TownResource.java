package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.SearchService;

import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

    private SearchService searchService;


    @Inject
    public TownResource() {
        searchService=new SearchService();
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {

        return searchService.suggestions(text);

    }

    @Get("location/:townName")
    public Double[] getLocation(String townName){
        return null;
    }
}
