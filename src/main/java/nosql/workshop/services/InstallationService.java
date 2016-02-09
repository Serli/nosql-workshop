package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import net.codestory.http.Query;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

import org.elasticsearch.common.collect.Lists;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

	private HashMap<String,CountByActivity> activitiesCounts = new HashMap<String,CountByActivity>();

	@Inject
	public InstallationService(MongoDB mongoDB) throws UnknownHostException {
		this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
	}

	public Installation random() {
		return installations.findOne().as(Installation.class);
	}

	public List<Installation> list(){
		Iterable<Installation> list = installations.find().as(Installation.class);
		return Lists.newArrayList(list);
	}

	public Installation get(String numero){
		return installations.findOne("{_id:'"+numero+"'}").as(Installation.class);
	}

	public List<Installation> search(String query){
		Iterable<Installation> list 
		= installations.find("{$text:{$search:'"+query+"'}}")
		.as(Installation.class);
		return Lists.newArrayList(list);
	}
	public List<Installation> geosearch(Query query){
		String lat = query.get("lat");
		String lng = query.get("lng");
		String distance = query.get("distance");
		String geoQuery = "{location: "
				+ "{$near: "
				+ "{$geometry :"
				+ "{type: \"Point\","
				+ "coordinates :["+lat+","+lng+"]"
				+ "},"
				+ "$maxDistance: "+distance+""
				+ "}"
				+ "}"
				+ "}";
		Iterator<Installation> inst 
		= installations.find(geoQuery)
		.as(Installation.class);
		return Lists.newArrayList(inst);
	}

	public InstallationsStats stats() {
		long equipementCount=0;
		int max = 0;
		String maxId="";
		long count = installations.count();
		Iterator<Installation> inst = installations.find().as(Installation.class);
		List<Installation> list = Lists.newArrayList(inst);
		for(Installation i : list){
			int equipementSize = i.getEquipements().size();
			equipementCount+=equipementSize;
			if(max < equipementSize){
				max = equipementSize;
				maxId = i.get_id();
			}


			for(Equipement e : i.getEquipements()){
				if(e != null && e.getActivites()!= null){
					for(String activity : e.getActivites()){
						incCountByActivity(activity);
						/*CountByActivity temp = getCountByActivity(activity);
						temp.setTotal(temp.getTotal()+1);

						/*if(countByActivities.contains(temp)){
							countByActivities.get(countByActivities.indexOf(temp)).increment();
						}
						else{
							countByActivities.add(temp.increment());
						}*/
					}
				}
			}
		}
		double averageEquipements = Double.valueOf(equipementCount)/Double.valueOf(count);
		Installation instWithMaxEquip = get(maxId);

		InstallationsStats ret = new InstallationsStats();
		ret.setAverageEquipmentsPerInstallation(averageEquipements);
		
		ArrayList<CountByActivity> counts = new ArrayList<CountByActivity>(activitiesCounts.values());
		Collections.sort(counts);
		ret.setCountByActivity(counts);
		
		ret.setTotalCount(count);
		ret.setInstallationWithMaxEquipments(instWithMaxEquip);
		return ret;
	}

	public CountByActivity getCountByActivity(String activity){
		if(activitiesCounts.containsKey(activity)){
			return activitiesCounts.get(activity);
		}
		else{
			CountByActivity ret = new CountByActivity();
			ret.setActivite(activity);
			ret.setTotal(0);
			return ret;
		}
	}
	public void incCountByActivity(String activity){
		if(activitiesCounts.containsKey(activity)){
			activitiesCounts.get(activity).setTotal(activitiesCounts.get(activity).getTotal()+1);
		}else{
			CountByActivity ret = new CountByActivity();
			ret.setActivite(activity);
			ret.setTotal(1);
			activitiesCounts.put(activity, ret);
		}
	}
}
