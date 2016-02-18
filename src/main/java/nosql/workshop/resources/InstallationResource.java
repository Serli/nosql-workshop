package nosql.workshop.resources;

import com.google.inject.Inject;

import net.codestory.http.Context;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.SearchService;

import java.util.List;

/**
 * Resource permettant de gérer l'accès à l'API pour les Installations.
 */
public class InstallationResource {

    private final InstallationService installationService;
    private final SearchService searchService;

    @Inject
    public InstallationResource(InstallationService installationService, SearchService searchService) {
        this.installationService = installationService;
        this.searchService = searchService;
    }

    @Get("/")
    @Get("/installations")
    public List<Installation> list(Context context) {
        return installationService.list();
    }

    @Get("/:numero")
    public Installation get(String numero) {
        return installationService.get(numero);
    }

    @Get("/random")
    public Installation random() {
        return installationService.random();
    }

    @Get("/search")
    public List<Installation> search(Context context) {
    	//return installationService.search(context.get("query"));
    	return searchService.search(context.get("query"));
    }

    @Get("/geosearch")
    public List<Installation> geosearch(Context context) {
    	return installationService.geoSearch(context.get("lat"), context.get("lng"), context.get("distance"));

    }

    @Get("/stats")
    public InstallationsStats stats() {
        return installationService.stats();

    }
}
