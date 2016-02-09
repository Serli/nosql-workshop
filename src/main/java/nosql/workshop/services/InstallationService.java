package nosql.workshop.services;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
        // FIXME : bien sûr ce code n'est pas le bon ... peut être quelque chose comme installations.findOne()
        Installation installation = this.installations.findOne().as(Installation.class);
//        installation.setNom("Mon Installation");
//        installation.setEquipements(Arrays.asList(new Equipement()));
//        installation.setAdresse(new Adresse());
//        Location location = new Location();
//        location.setCoordinates(new double[]{3.4, 3.2});
//        installation.setLocation(location);
        return installation;
    }
    
    public List<Installation> list() throws IOException {
    	MongoCursor<Installation> all = installations.find().as(Installation.class);
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
    
    public List<Installation> search(Context context) {
    	String query = context.get("query");
    	//List<Installation> installations =
    	List<Installation> list = Lists.newArrayList(
    			(Iterator<Installation>)
    			this.installations.aggregate("{$project:{sender:1}}")
    			.and("{$match:{tags:'#'}}", query)
    			.and("{$limit:10}")
    			.as(Installation.class));
        return list;
    }
}
