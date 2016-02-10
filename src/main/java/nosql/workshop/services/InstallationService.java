package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import net.codestory.http.Context;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
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
	
	public List<Installation> search(String string) {
		List<Installation> installationList = new ArrayList<Installation>();
		MongoCursor<Installation> cursor = this.installations.find("{'$text': {'$search': '"+string+"','$language' : 'french'}}")
				.projection("{'score': {'$meta': 'textScore'}}")
				.sort("{'score': {'$meta': 'textScore'}}")
				.limit(10)
				.as(Installation.class);
		while (cursor.hasNext()) {
			installationList.add(cursor.next());
		}
		return installationList;
	}

	public List<Installation> geosearch(String lat, String lng, String distance) {
		List<Installation> installationList = new ArrayList<Installation>();
		MongoCursor<Installation> cursor = this.installations.find("{ 'location' : { $near : { $geometry : { type : 'Point' ,coordinates : [ "+lng+" , "+lat+" ]}, $maxDistance : "+distance+"}}}").as(Installation.class);
		while (cursor.hasNext()) {
			installationList.add(cursor.next());
		}
		return installationList;
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

	public InstallationsStats stats(){
		InstallationsStats stats = new InstallationsStats();

		Iterator<CountByActivity> iterator = this.installations.aggregate("{$group: { _id: null, total : {$sum : 1}}}").as(CountByActivity.class);
		while(iterator.hasNext()) {
			CountByActivity count = iterator.next();
			stats.setTotalCount(count.getTotal());
		}

		iterator = this.installations.aggregate("{ $unwind : \"$equipements\" }")
				.and("{ $unwind : \"$equipements.activites\" }")
				.and("{ $group : { _id : \"$equipements.activites\", total : {$sum : 1}}}")
				.and("{ $project : { _id : 0, activite : \"$_id\", total : 1 }}")
				.as(CountByActivity.class);
		List<CountByActivity> listAct = new ArrayList<CountByActivity>();
		while(iterator.hasNext()) {
			CountByActivity countAct = iterator.next();
			listAct.add(countAct);
		}
		stats.setCountByActivity(listAct);

		Iterator<Installation> iteInst = this.installations.aggregate("{ $project : { _id : 1, nom : 1, adresse : 1, multiCommune : 1, nbPlacesParking : 1, nbPlacesParkingHandicapes : 1, dateMiseAJourFiche : 1, equipements : 1, taille : {$size : \"$equipements\"} }}")
				.and("{ $sort: { \"taille\" : -1 } }")
				.as(Installation.class);
		Installation inst = iteInst.next();
		stats.setInstallationWithMaxEquipments(inst);

		Iterator<Average> iteAve = this.installations.aggregate("{ $group : { _id : null, avgSize : { $avg: {$size : \"$equipements\"} } } }")
				.and("{ $project : { _id : 0, average : \"$avgSize\" }}")
				.as(Average.class);
		Average ave = iteAve.next();
		stats.setAverageEquipmentsPerInstallation(ave.getAverage());

		return stats;
	}
}