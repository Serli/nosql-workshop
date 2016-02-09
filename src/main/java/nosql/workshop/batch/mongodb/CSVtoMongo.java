package nosql.workshop.batch.mongodb; /*
 * ${FILE_NAME}
 * author:   Maxime Perocheau
 * created:  2016 février 08 @ 17:22
 * modified: 2016 février 08 @ 17:22
 *
 * TODO : description
 */

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class CSVtoMongo {

    MongoClient mongoClient = new MongoClient();
    DB db = mongoClient.getDB("nosql-workshop");


    //"Nom usuel de l'installation","Numéro de l'installation","Nom de la commune","Code INSEE","Code postal","Nom du lieu dit","Numero de la voie","Nom de la voie","location","Longitude","Latitude","Aucun aménagement d'accessibilité","Accessibilité handicapés à mobilité réduite","Accessibilité handicapés sensoriels","Emprise foncière en m2","Gardiennée avec ou sans logement de gardien","Multi commune","Nombre total de place de parking","Nombre total de place de parking handicapés","Installation particulière","Desserte métro","Desserte bus","Desserte Tram","Desserte train","Desserte bateau","Desserte autre","Nombre total d'équipements sportifs","Nombre total de fiches équipements","Date de mise à jour de la fiche installation"

}
