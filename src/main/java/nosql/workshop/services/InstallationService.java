package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import org.bson.types.ObjectId;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {
    private static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = new MongoDB().getJongo().getCollection(COLLECTION_NAME);
    }

    public List<Installation> getAll() {
        Iterator<Installation> it = installations.find().as(Installation.class).iterator();
        List<Installation> result = new ArrayList<>();
        while (it.hasNext()) {
            result.add(it.next());
        }

        return result;
    }

    public Installation getById(String id) {
        return installations.findOne("{_id:'" + id + "'}").as(Installation.class);
    }

    public List<Installation> get(String query) {

        return null;
    }

    public Installation getRandom() {
        // ok for this exercise because we don't insert so many documents
        int count = (int)installations.count();
        Random rnd = new Random(System.currentTimeMillis());
        // nextInt choose between 0 and bound excluded, then we want to skip n-1 docs to choose the nth
        int nbToSkip = rnd.nextInt(count);
        Iterator<Installation> it = installations.find().skip(nbToSkip).as(Installation.class).iterator();
        // by construction, there is necessarily something in next()
        return it.next();
    }
}
