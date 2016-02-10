package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.Context;
import net.codestory.http.annotations.Get;
import net.codestory.http.errors.NotFoundException;
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
    public List<Installation> list(Context context) {
        List<Installation> results = installationService.getAll();
        return results;
    }

    @Get("/:numero")
    public Installation get(String numero) {
        Installation result = installationService.getById(numero);

        if (result == null) {
            return NotFoundException.notFoundIfNull(result);
        }

        return result;
    }


    @Get("/random")
    public Installation random() {
        return installationService.getRandom();
    }

    @Get("/search")
    public List<Installation> search(Context context) {
        String query = context.get("query");
        try {
            return searchService.get(query);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    @Get("/geosearch")
    public List<Installation> geosearch(Context context) {
        String lat = context.get("lat");
        String lng = context.get("lng");
        String distance = context.get("distance");

        // TODO bad request
//        if (lat == null || lng == null | distance == null) {
//            throw new BadRequestException();
//        }

        return installationService.getGeoSearchResults(lat, lng, distance);

    }

    @Get("/stats")
    public InstallationsStats stats() {
        return installationService.getStats();
    }
}
