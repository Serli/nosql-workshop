package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.Arrays;

import static nosql.workshop.model.Installation.*;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {
    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
        // FIXME : bien sûr ce code n'est pas le bon ... peut être quelque chose comme installations.findOne()
        Installation installation = new Installation();
        installation.setNom("Mon Installation");
        installation.setEquipements(Arrays.asList(new Equipement()));
        installation.setAdresse(new Adresse());
        Location location = new Location();
        location.setCoordinates(new double[]{3.4, 3.2});
        installation.setLocation(location);
        return installation;
    }
}
