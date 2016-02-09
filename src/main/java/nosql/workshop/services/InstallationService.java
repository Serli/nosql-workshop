package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static nosql.workshop.model.Installation.*;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {
    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installation";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {

        Installation installation = new Installation();
        Installation installationR = installations.findOne().as(Installation.class);
        installation.setNom(installationR.get_id());
        installation.setNom(installationR.getNom());
        installation.setEquipements(installationR.getEquipements());
        installation.setAdresse(installationR.getAdresse());
        Location location = new Location();
        location.setCoordinates(installationR.getLocation().getCoordinates());
        installation.setLocation(installationR.getLocation());
        return installation;
    }

    public List<Installation> list() {
        return new ArrayList<>();
    }

    public Installation get(String numero) {
        return new Installation();
    }

    public InstallationsStats stats() {
        return new InstallationsStats();
    }

    public Double[] getLocation(String townName) {
        return new Double[]{3.4, 3.2};
    }
}
