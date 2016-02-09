package nosql.workshop.services;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.FindOne;
import org.jongo.MongoCollection;
import net.codestory.http.Context;

import java.net.UnknownHostException;
import java.util.List;

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

        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();

        //Installation installation = new Installation();
        //installation.setNom("Mon Installation");
        //installation.setEquipements(Arrays.asList(new Equipement()));
        //installation.setAdresse(new Adresse());
        //Location location = new Location();
       //location.setCoordinates(new double[]{3.4, 3.2});
        //installation.setLocation(location);
        //return installation;
    }

    public Installation getById(String Id) {
        return installations.findOne("{_id : '"+Id+"'}").as(Installation.class);
    }

    public List<Installation> getAll() {
        return Lists.newArrayList(installations.find().as(Installation.class).iterator());
    }

    public InstallationsStats getStats(){
        return null;
    }

    public  List<Installation> geoSearch(String info) {
        return null;
    }

    public List<Installation> search(String info) {
        //return Lists.newArrayList(installations.find());
        return null;
    }

}
