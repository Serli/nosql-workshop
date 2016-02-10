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
    public InstallationResource(InstallationService installationService,
                                SearchService searchService) {
        this.installationService = installationService;
        this.searchService = searchService;
    }


    @Get("/")
    @Get("")
    public List<Installation> list(Context context) {
        return installationService.getAllInstallations();
    }

    @Get("/:numero")
    public Installation get(String numero) {
        return installationService.getInstallation(numero);
    }


    @Get("/random")
    public Installation random() {
        return installationService.getRandomInstallation();
    }

    @Get("/search")
    public List<Installation> search(Context context) {
        String query = context.query().get("query");
        return installationService.searchInstallations(query);

    }

    @Get("/geosearch")
    public List<Installation> geosearch(Context context) {
        float lat = context.query().getFloat("lat");
        float lng = context.query().getFloat("lng");
        int distance = context.query().getInteger("distance");
        return installationService.getInstallationByGeoSearch(lat, lng, distance);

    }

    @Get("/stats")
    public InstallationsStats stats() {
        return installationService.getStats();
    }
}
