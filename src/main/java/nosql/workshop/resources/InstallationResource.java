package nosql.workshop.resources;

import com.google.inject.Inject;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import net.codestory.http.Context;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import nosql.workshop.services.InstallationService;

import java.io.IOException;
import java.util.List;

import org.jongo.MongoCursor;

/**
 * Resource permettant de gérer l'accès à l'API pour les Installations.
 */
public class InstallationResource {

    private final InstallationService installationService;

    @Inject
    public InstallationResource(InstallationService installationService) {
        this.installationService = installationService;
    }


    @Get("/")
    @Get("")
    public List<Installation> list(Context context) {
    	return installationService.getAllInstallations();
    }

    @Get("/:numero")
    public Installation get(String numero) {    	
    	return installationService.getInstallationByNumero(numero);
    }


    @Get("/random")
    public Installation random() {
        return installationService.random();
    }

    @Get("/search")
    public List<Installation> search(Context context) throws IOException {
    	String query = context.query().get("query");
    	return installationService.search(query);
    }

    @Get("/geosearch")
    public List<Installation> geosearch(Context context) throws IOException {
    	String lat = context.query().get("lat");
    	String lng = context.query().get("lng");
    	String distance = context.query().get("distance");
    	return installationService.geosearch(lat, lng, distance);
    }

    @Get("/stats")
    public InstallationsStats stats() {
        return installationService.getInstallationsStat();

    }
}
