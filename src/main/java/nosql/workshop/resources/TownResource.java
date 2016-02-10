package nosql.workshop.resources;

import com.google.inject.Inject;

import net.codestory.http.annotations.Get;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.SearchService;

import java.util.List;

/**
 * API REST qui expose les services liés aux villes
 */
public class TownResource {

	private final InstallationService installationService;

    @Inject
    public TownResource(InstallationService installationService) {
        this.installationService = installationService;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {
    	return installationService.suggest(text);
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName){
        return installationService.getLocation(townName);
    }
}
