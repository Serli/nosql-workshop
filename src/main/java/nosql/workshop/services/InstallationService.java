package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.BasicDBObject;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import org.bson.types.ObjectId;
import org.jongo.MongoCollection;
import org.jongo.marshall.jackson.oid.MongoId;

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
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
        Installation installation = new Installation();
        Installation installationRandom = installations.findOne().as(Installation.class);
        installation.setNom(installationRandom.getNom());
        installation.setEquipements(installationRandom.getEquipements());
        installation.setAdresse(installationRandom.getAdresse());
        Location location = new Location();
        location.setCoordinates(installationRandom.getLocation().getCoordinates());
        installation.setLocation(installationRandom.getLocation());
        return installation;
    }

    public Installation get(String numero) {
        Installation installation = new Installation();
        Installation installationId = installations.findOne("{_id : '" + numero + "'}").as(Installation.class);
        installation.setNom(installationId.getNom());
        installation.setEquipements(installationId.getEquipements());
        installation.setAdresse(installationId.getAdresse());
        Location location = new Location();
        location.setCoordinates(installationId.getLocation().getCoordinates());
        installation.setLocation(installationId.getLocation());
        installation.set_id(numero);
        installation.setDateMiseAJourFiche(installationId.getDateMiseAJourFiche());
        installation.setMultiCommune(installationId.isMultiCommune());
        installation.setNbPlacesParking(installationId.getNbPlacesParking());
        installation.setNbPlacesParkingHandicapes(installation.getNbPlacesParkingHandicapes());
        return installation;
    }

    public List<Installation> list() {
        List<Installation> listInstallation = new ArrayList<Installation>();
        return listInstallation;
    }
}
