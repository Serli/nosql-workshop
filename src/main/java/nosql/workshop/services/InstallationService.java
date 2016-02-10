package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

import com.mongodb.BasicDBObject;
import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Installation.Adresse;
import nosql.workshop.model.Installation.Location;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

import org.bson.BasicBSONDecoder;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

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
    	long nombre = this.installations.count();
        MongoCursor<Installation> installation = this.installations.find().skip((int) (Math.random()*nombre)).as(Installation.class);
        return installation.next();
    }
    
    public List<Installation> list() throws IOException {
    	MongoCursor<Installation> all = this.installations.find().as(Installation.class);
    	List<Installation> list = new ArrayList<Installation>();
    	try {
			while(all.hasNext()) {
				Installation inst = all.next();
				list.add(inst);
			}
		} finally {
			all.close();
		}
    	return list;
    }
    
    public Installation get(String numero) {
    	Installation installation = this.installations.findOne("{ \"_id\" : # }", numero).as(Installation.class);
    	return installation;
    }
    
    public List<Installation> search(Context context) throws IOException {
    	String query = context.query().get("query");

    	MongoCursor<Installation> all = this.installations.find("{"
    			+ "$text: {"
    			+ "$search : #,"
    			+ "$language : \"french\""
    			+ "}"
    			+ "}"
    			+ "}"
    			+ "}", query).projection("{score:{$meta: \"textScore\"}}").sort("{score:{$meta: \"textScore\"}}").limit(10).as(Installation.class);

    	
    	List<Installation> list = new ArrayList<Installation>();
    	try {
			while(all.hasNext()) {
				Installation inst = all.next();
				list.add(inst);
			}
		} finally {
			all.close();
		}
        return list;
    }

	public List<Installation> geosearch(Context context) {

        List<Installation> list = new ArrayList<>();
        MongoCursor<Installation> cursor = this.installations.find("{ \"location\" : " +
                        "{ $near : " +
                        "{ $geometry :" +
                        "{ type : \"Point\" ," +
                        "  coordinates : [ #, # ]" +
                        "}," +
                        "$maxDistance : # " +
                        "}}}",
                Double.parseDouble(context.query().get("lng")),
                Double.parseDouble(context.query().get("lat")),
                Double.parseDouble(context.query().get("distance")))
                .as(Installation.class);

        try {
            while(cursor.hasNext()) {
                Installation inst = cursor.next();
                list.add(inst);
            }
        } finally {
            try {
                cursor.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
	}
	
	public InstallationsStats stats() throws IOException {
    	InstallationsStats stats = new InstallationsStats();
    	
    	Iterator<CountByActivity> ite = this.installations.aggregate("{$group: { _id: null, total : {$sum : 1}}}").as(CountByActivity.class);
    	CountByActivity count = ite.next();
    	stats.setTotalCount(count.getTotal());

    	ite = this.installations.aggregate("{ $unwind : \"$equipements\" }")
    			.and("{ $unwind : \"$equipements.activites\" }")
    			.and("{ $group : { _id : \"$equipements.activites\", total : {$sum : 1}}}")
    			.and("{ $project : { _id : 0, activite : \"$_id\", total : 1 }}")
    			.as(CountByActivity.class);
    	List<CountByActivity> listAct = new ArrayList<CountByActivity>();
    	while(ite.hasNext()) {
    		CountByActivity countAct = ite.next();
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
