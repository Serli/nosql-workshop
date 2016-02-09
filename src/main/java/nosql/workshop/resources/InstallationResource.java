package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.Context;
import net.codestory.http.Query;
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
    @Get("")
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
        String query = context.query().get("query");
        return searchService.search(query);
    }

    @Get("/geosearch")
    public List<Installation> geosearch(Context context) {
        Query query = context.query();
        Double lat = query.getDouble("lat");
        Double lng = query.getDouble("lng");
        Integer distance = query.getInteger("distance");
        return installationService.geosearch(lat, lng, distance);
    }

    @Get("/stats")
    public InstallationsStats stats() {
        return installationService.stats();
    }
}
