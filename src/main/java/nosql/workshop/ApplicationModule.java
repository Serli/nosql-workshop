package nosql.workshop;

import com.google.inject.AbstractModule;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.MongoDB;
import nosql.workshop.services.SearchService;
import nosql.workshop.services.TownService;


/**
 * Module Guice permettant de définir les classes pouvant être injectées.
 */
public class ApplicationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(MongoDB.class);
        bind(InstallationService.class);
        bind(SearchService.class);
        bind(TownService.class);
    }
}
