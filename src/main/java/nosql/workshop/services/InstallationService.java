package nosql.workshop.services;

import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import org.jongo.Jongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    public Installation getById() {

        return null;
    }

    public List<Installation> get() {

        return null;
    }
}
