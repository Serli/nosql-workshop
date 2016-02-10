package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
    public static final String COLLECTION_NAME = "collectionInstallationsSportives";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
    	
    	return installations.findOne().as(Installation.class);
    }
    
    public List<Installation> list(Context context) {
    	List<Installation> liste = new ArrayList<Installation>();
    	Consumer<Installation> action = (Installation s) -> liste.add(s);
    	installations.find().as(Installation.class).forEach(action);
        return liste;
    	
    }
    
    public Installation get(String number){
    	Installation installation = installations.findOne(String.format("{_id: '%s'}", number)).as(Installation.class);
    	return installation;
    }
    
    
    public List<Installation> search(Context context){
      	List<Installation> liste = new ArrayList();
        String query = context.get("query");
        String[] arr = query.split(" ");
        MongoCursor<Installation>[] collection= new MongoCursor[arr.length];
        for (int i=0;i<arr.length;i++){
        	collection[i] =installations.find(String.format("{nom: '%s'}", arr[0])).as(Installation.class);
        	//TODO rajouter d'autres critères de recherche.
        }
        for(int j=0;j<collection.length;j++){
        	for(int h=0;h<collection[j].count();h++){
        		liste.add(collection[j].next());
        	}
        }
      	return liste;
    }
    
    public List<Installation> geoSearch(Context context) {
		String lat = context.get("lat");
		String lng = context.get("lng");
		String distance = context.get("distance");
		installations.ensureIndex(" { \"location\" : \"2dsphere\" } ");
		MongoCursor<Installation> collection= installations.find(
				"{ \"location\" : "
						+ "{ $near : "
						+	 "{ $geometry : "
						+ 		"{ type : \"Point\" , "
						+ 		" coordinates : ["+lng+" , "+lat+"]"
						+ 		"} ,"
						+ 		" $maxDistance : "+distance+""
						+ 	"}"
						+ "}"
						+ "})"
				).as(Installation.class);
		
		
		List<Installation> aretourner = new ArrayList<Installation>();
		
		for(int h=0;h<collection.count();h++){
			aretourner.add(collection.next());
		}
		return aretourner;
	}
    
    public InstallationsStats stats(){
    	long total = installations.count();//nb d'installations
    	List<Installation> liste = new ArrayList<Installation>();
    	Consumer<Installation> action = (Installation s) -> liste.add(s);
    	installations.find().as(Installation.class).forEach(action);
        List<String> listeActivite= new ArrayList<String>();
        int k=liste.get(0).getEquipements().size();
        int equipementtotal=0;
        for (int i=0;i<liste.size();i++){
        	if(liste.get(i).getEquipements()!=null){
        		equipementtotal=equipementtotal+liste.get(i).getEquipements().size();
        		if(liste.get(i).getEquipements().size()>liste.get(k).getEquipements().size()){
        			k=i;}
        	for(int j=0;j<liste.get(i).getEquipements().size();j++){
            	
        		
        	
        		if(liste.get(i).getEquipements().get(j).getActivites()!=null){
        		
        		for(int h=0;h<liste.get(i).getEquipements().get(j).getActivites().size();h++){
        			listeActivite.add(liste.get(i).getEquipements().get(j).getActivites().get(h));
        		}}
        	}}
        }
        List<CountByActivity> listCount = new ArrayList<CountByActivity>();
        CountByActivity count;
        
        	while(listeActivite.size()>0){
        		int j=1;
        		for(int i=1;i<listeActivite.size();i++){
        			
        			while(i<listeActivite.size()&&listeActivite.get(i).contains((listeActivite.get(0)))){
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
        
        listCount.sort(new Comparator<CountByActivity>(){

			@Override
			public int compare(CountByActivity arg0, CountByActivity arg1) {
				if(arg0.getTotal()>arg1.getTotal()){return -1;}
				if(arg0.getTotal()<arg1.getTotal()){return 1;}
				return 0;
			}
        		
        });
        double average = (double) equipementtotal/total;
        InstallationsStats stats = new InstallationsStats();
        stats.setAverageEquipmentsPerInstallation(average);
        stats.setCountByActivity(listCount);
        stats.setInstallationWithMaxEquipments(liste.get(k));
        stats.setTotalCount(total);
        return stats;
    }
}
