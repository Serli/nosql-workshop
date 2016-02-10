package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.DBObject;

import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

import org.elasticsearch.action.count.CountAction;
import org.jongo.Aggregate;
import org.jongo.Aggregate.ResultsIterator;
import org.jongo.FindOne;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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
		return installations.findOne().as(Installation.class);
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
		InstallationsStats i = new InstallationsStats();
		ResultsIterator<CountByActivity> allTotal = this.installations.aggregate("{$group: {_id: null, total: {$sum: 1}}}").as(CountByActivity.class);
		
		while (allTotal.hasNext()) {
			CountByActivity cByA = allTotal.next();
			i.setTotalCount(cByA.getTotal());
		}
		
		Iterator<CountByActivity> allActivities = this.installations.aggregate("{ $unwind: \"$equipements\" }")
    			.and("{ $unwind: \"$equipements.activites\" }")
    			.and("{ $group: { _id: \"$equipements.activites\", total: {$sum: 1}}}")
    			.and("{ $project: { _id: 0, activite: \"$_id\", total: 1 }}")
    			.as(CountByActivity.class);
		
    	List<CountByActivity> list = new ArrayList<CountByActivity>();
    	
    	while(allActivities.hasNext()) {
    		CountByActivity countAct = allActivities.next();
    		list.add(countAct);
    	}
    	i.setCountByActivity(list);
    	
    	Iterator<Average> allEquipements = this.installations.aggregate("{$group: {_id: null, average: {$avg: {$size: \"$equipements\"}}}}")
				.as(Average.class);
		 
    	Average average = allEquipements.next();
		i.setAverageEquipmentsPerInstallation(average.getAverage());

		Iterator<Installation> max = this.installations.aggregate("{$project: {_id : 1, nom : 1, adresse : 1, multiCommune : 1, nbPlacesParking : 1, nbPlacesParkingHandicapes : 1, dateMiseAJourFiche : 1, equipements : 1, taille : {$size : \"$equipements\"}}}")
				.and("{$sort: {\"taille\": -1}}")
				.as(Installation.class);
		
		i.setInstallationWithMaxEquipments(max.next());
		
		return i;
	}
}
