package nosql.workshop.services;

import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import org.bson.types.ObjectId;
import org.jongo.Jongo;

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
    private Jongo mongo;

    public InstallationService() throws UnknownHostException {
        this.mongo = new MongoDB().getJongo();
    }

    public List<Installation> getAll() {
        Iterator<Installation> it = mongo.getCollection("installations").find().as(Installation.class).iterator();
        List<Installation> result = new ArrayList<>();
        while (it.hasNext()) {
            result.add(it.next());
        }

        return result;
    }

    public Installation getById(String id) {
        return mongo.getCollection("installations").findOne("{_id:'" + id + "'}").as(Installation.class);
    }

    public List<Installation> get(String query) {

        return null;
    }

    public Installation getRandom() {
        // ok for this exercise because we don't insert so many documents
        int count = (int)mongo.getCollection("installations").count();
        Random rnd = new Random(System.currentTimeMillis());
        // nextInt choose between 0 and bound excluded, then we want to skip n-1 docs to choose the nth
        int nbToSkip = rnd.nextInt(count);
        Iterator<Installation> it = mongo.getCollection("installations").find().skip(nbToSkip).as(Installation.class).iterator();
        // by construction, there is necessarily something in next()
        return it.next();
    }
}
