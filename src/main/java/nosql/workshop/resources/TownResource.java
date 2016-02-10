package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.annotations.Get;
import net.codestory.http.errors.NotFoundException;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.SearchService;

import java.io.IOException;
import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

    private final SearchService service;

    @Inject
    public TownResource(SearchService service) {
        this.service = service;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {
        try {
            return service.suggest(text);
        } catch (IOException e) {
            throw new NotFoundException();
        }
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName){
        try {
            return service.getLocation(townName);
        } catch (IOException e) {
            throw new NotFoundException();
        }
    }
}
