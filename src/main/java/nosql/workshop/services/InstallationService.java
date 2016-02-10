package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.BasicDBObject;

import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

import org.jongo.FindOne;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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
    public static final String COLLECTION_NAME = "collectionInstallationsSportives";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
    	return installations.findOne().as(Installation.class);
    }
    
    public Installation byId(String numero) {
    	return installations.findOne("{\"_id\": \""+numero+"\"}").as(Installation.class);
    }
    
    public List<Installation> all(Context context) {
    	MongoCursor<Installation> cursor = installations.find().as(Installation.class);
    	List<Installation> aRet = new ArrayList();
    	for(Installation ins : cursor) {
    		aRet.add(ins);
    	}
		return aRet;
    }
    
    public List<Installation> search(Context context){
      	List<Installation> liste = new ArrayList();
        String query = context.get("query");
        String[] arr = query.split(" ");
        MongoCursor<Installation>[] collection= new MongoCursor[arr.length];
        for (int i=0;i<arr.length;i++){
        	collection[i] =installations.find("{nom: '"+arr[0]+"'}").as(Installation.class);
        }
        for(int j=0;j<collection.length;j++){
        	for(int h=0;h<collection[j].count();h++){
        		liste.add(collection[j].next());
        	}
        }
      	return liste;
    }

	public List<Installation> geoSearch(Context context) {
		List<Installation> liste = new ArrayList();
		String lat = context.get("lat");
		String lng = context.get("lng");
		String distance = context.get("distance");
		installations.ensureIndex("{'coordinates':'2d'},{'coordinates':'2d'}");
		MongoCursor<Installation> collection= installations.find("{'coordinates':{'$near':["+lat+","+lng+"],$maxDistance:"+distance+"}}})").as(Installation.class);
        for(Installation ins : collection){
        		liste.add(ins);
        }
      	return liste;
	}
	
	public InstallationsStats stats(){
    	long total = installations.count();
    	List<Installation> liste = new ArrayList();
    	MongoCursor<Installation> collection= installations.find().as(Installation.class);
        for(Installation ins : collection){
        		liste.add(ins);
        }
        List<String> listeActivite= new ArrayList();
        int k=0;
        int equipementtotal=0;
        for (int i=0;i<liste.size();i++){
        	List<Equipement> equips = liste.get(i).getEquipements();
        	if(equips!=null){
        	for(int j=0;j<equips.size();j++){
        		equipementtotal=equipementtotal+equips.size();
        		if(equips.size()>k){
        			k=i;}
        		for(int h=0;h<equips.get(j).getActivites().size();h++){
        			listeActivite.add(equips.get(j).getActivites().get(h));
        		}
        	}}
        }
        List<CountByActivity> listCount = new ArrayList();
        CountByActivity count;
        	while(listeActivite.size()>0){
        		int j=1;
        		for(int i=1;i<listeActivite.size();i++){
        			if(listeActivite.get(i).matches(listeActivite.get(0))){
        				j++;
        				listeActivite.remove(i);
        			}
        		}
        		count = new CountByActivity();
    			count.setActivite(listeActivite.get(0));
    			count.setTotal(j);
    			listeActivite.remove(0);
    			listCount.add(count);
        	}
        double average = (double) equipementtotal/total;
        InstallationsStats stats = new InstallationsStats();
        stats.setAverageEquipmentsPerInstallation(average);
        stats.setCountByActivity(listCount);
        stats.setInstallationWithMaxEquipments(liste.get(k));
        stats.setTotalCount(total);
        return stats;
    }
}
