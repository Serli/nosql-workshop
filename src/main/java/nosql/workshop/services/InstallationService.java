package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;

import org.jongo.Find;
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
		long n = installations.count();
		MongoCursor<Installation> all = installations.find().limit(1).skip((int)Math.floor(Math.random()*n)).as(Installation.class);
		return all.count() > 0 ? all.next() : null;
		//return installations.findOne().as(Installation.class);
	}

	public List<Installation> list() {
		MongoCursor<Installation> all =  installations.find().as(Installation.class);
		List<Installation> installations = new ArrayList<Installation>();
		while (all.hasNext()) {
			installations.add(all.next());
		}
		return installations;
	}

	public List<Installation> search(String query) {
		MongoCursor<Installation> all = installations.find("{$text:{ $search: '" + query + "'}}").as(Installation.class);
		List<Installation> installations = new ArrayList<Installation>();
		while (all.hasNext()) {
			installations.add(all.next());
		}
		return installations;
	}

	public List<Installation> geoSearch(String lat, String lng, String dist) {
		MongoCursor<Installation> all = installations.find(String.format("{location: {$near:{$geometry: {type: 'Point', coordinates: [%s, %s]}, $maxDistance: %s}}}", lat, lng, dist))
				.as(Installation.class);
		List<Installation> installations = new ArrayList<Installation>();
		while (all.hasNext()) {
			installations.add(all.next());
		}
		return installations;
	}

	public Installation get(String numero) {
		return installations.findOne("{_id: '" + numero + "'}").as(Installation.class);
	}

	public InstallationsStats stats() {
		/*InstallationsStats i = new InstallationsStats();
		i.getTotalCount();
		ResultsIterator<InstallationsStats> all = installations.aggregate("{totalCount: {$sum : 1}, countByActivity: [{$group : {activite : '$equipements.activites', total : {$sum : 1}}}]}").as(InstallationsStats.class);
		while (all.hasNext()) {
			installations.add(all.next());
		}
		return installations;*/
		return null;
	}
}
