package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.Context;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.SearchService;

import java.io.IOException;
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
    public List<Installation> list(Context context) throws IOException {
        return this.installationService.list();
        // TODO make it work
        //return this.searchService.list();
    }

    @Get("/:numero")
    public Installation get(String numero) {
        return this.installationService.get(numero);
    }


    @Get("/random")
    public Installation random() {
        return this.installationService.random();
    }

    @Get("/search")
    public List<Installation> search(Context context) throws IOException {
        return this.installationService.search(context);
    }

    @Get("/geosearch")
    public List<Installation> geosearch(Context context) {
        return this.installationService.geosearch(context);

    }

    @Get("/stats")
    public InstallationsStats stats() throws IOException {
        return this.installationService.stats();

    }
}
