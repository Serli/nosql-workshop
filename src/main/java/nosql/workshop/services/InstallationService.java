package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import net.codestory.http.Context;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;

import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
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
        return this.installations.findOne().as(Installation.class);
    }

	public List<Installation> search(Context context) {
		
		return null;
	}

	public List<Installation> list(Context context) {
		List<Installation> installation = new ArrayList<Installation>();
		MongoCursor<Installation> cursor = this.installations.find().as(Installation.class);
		while (cursor.hasNext()) {
			installation.add(cursor.next());
		}
		return installation;
	}

	public Installation get(String numero) {
		return this.installations.findOne("{_id:'"+numero+"'}").as(Installation.class);
	}

	public InstallationsStats stats() {
		InstallationsStats instStats = new InstallationsStats();
		instStats.setTotalCount(this.installations.count());
		return instStats;
	}
}