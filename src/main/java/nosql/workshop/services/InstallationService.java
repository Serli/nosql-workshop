package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.BasicDBObject;

import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

import org.jongo.Find;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.omg.CORBA.SystemException;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

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
    	int nb = (int) installations.count();
    	Installation installation = installations.find().skip((int) (Math.random()*nb + 1)).as(Installation.class).next();
    	return installation;
    }
    
    public List<Installation> getAll(){
    	List<Installation> installationsArray = new ArrayList<Installation>();
    	Consumer<Installation> action = (Installation s) -> installationsArray.add(s);
    	
    	installations.find().as(Installation.class).forEach(action);
    	
    	return installationsArray;
    }
    
    public Installation get(String number){
    	Installation installation = installations.findOne(String.format("{_id: '%s'}", number)).as(Installation.class);
    	return installation;
    }
    
    public List<Installation> geosearch(Context context) {
    	double lat = Double.parseDouble(context.get("lat"));
    	double lng = Double.parseDouble(context.get("lng"));
    	int distance = Integer.parseInt(context.get("distance"));
    	
    	List<Installation> installationsArray = new ArrayList<Installation>();
    	Consumer<Installation> action = (Installation s) -> installationsArray.add(s);
    	
    	installations.find(String.format("{\"location\": {$near : {$geometry : { type : \"Point\", coordinates: [%s, %s]}, $maxDistance : %s}}}",lat, lng, distance)).as(Installation.class).forEach(action);
    	return installationsArray;
    }
    
    public InstallationsStats stats(){
    	int totalCount = (int) installations.count();
    	// installations.aggregate("[{$project: { equipements: 1}} $group: { _id: \"$department\",average: { $avg: \"$sum\" } }]").as(Installation.class);
    	
    	double avg_equ = 0;
    	int max_equ = 0;
    	
    	ArrayList<Installation> list = new ArrayList<Installation>();
    	
    	Iterator<CountByActivity> countbyActivitiesIt = installations.aggregate(
				"{$match: {\"equipements.activites\":{$exists : true}}}")
				.and("{$unwind: \"$equipements\"}")
				.and("{$unwind: \"$equipements.activites\"}")
				.and("{$group: {_id:null,activite:{'$first'unsure emoticon\"$equipements.activites\"}, total: {$sum:1}}}")
				.and("{$sort: {total: -1}}").as(CountByActivity.class);
    	ArrayList<CountByActivity> activities = Lists.newArrayList(countbyActivitiesIt);
    	
    	Consumer<Installation> action = (Installation s) -> list.add(s);
    	installations.find().as(Installation.class).forEach(action);
    	
    	for (int i = 0 ; i < list.size() ; i++){
    		avg_equ += list.get(i).getEquipements().size();
    		if (list.get(i).getEquipements().size() > max_equ){
    			max_equ = list.get(i).getEquipements().size();
    		}
    		/*for (Equipement equ : list.get(i).getEquipements()){
    			for (String a : equ.getActivites()){
    				
    			}
    		}*/
    	}
    	avg_equ = avg_equ/totalCount;
    	
    	Installation max_install = installations.findOne(String.format("{_nbequ: '%s'}", max_equ)).as(Installation.class);
    	
    	InstallationsStats stats = new InstallationsStats();
    	stats.setAverageEquipmentsPerInstallation(avg_equ);
    	stats.setTotalCount(totalCount);
    	stats.setInstallationWithMaxEquipments(max_install);
    	stats.setCountByActivity(activities);
    	return stats;
    }
}
