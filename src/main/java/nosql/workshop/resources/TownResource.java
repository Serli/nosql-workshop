package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.TownService;

import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

    private final TownService townService;

    @Inject
    public TownResource(TownService townService) {
        this.townService = townService;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {
        return townService.suggest(text);
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName){
        return townService.getLocation(townName);
    }
}
