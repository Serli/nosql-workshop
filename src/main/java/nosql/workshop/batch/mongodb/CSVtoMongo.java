package nosql.workshop.batch.mongodb; /*
 * ${FILE_NAME}
 * author:   Maxime Perocheau
 * created:  2016 février 08 @ 17:22
 * modified: 2016 février 08 @ 17:22
 *
 * TODO : description
 */

import com.mongodb.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

public class CSVtoMongo {

    private static String pathInstallation = "./src/main/resources/batch/csv/installations.csv";
    private static String pathEquipements = "./src/main/resources/batch/csv/equipements.csv";
    private static String pathActivites = "./src/main/resources/batch/csv/activites.csv";

    public static void main (String[] args) {

        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection collection = db.getCollection("installations");

        collection.drop();

        remplissageInstallations(collection);
        remplissageEquipements(collection);
        remplissageActivites(collection);
    }

    public static void remplissageInstallations(DBCollection collection){
        for (Map<String, String> line : ReadCVS.run(pathInstallation)) {
            DBObject document = new BasicDBObject()
                    .append("_id", line.get("Numéro de l'installation"))
                    .append("nom", line.get("Nom usuel de l'installation"))
                    .append("adresse",
                            new BasicDBObject()
                                    .append("numero", line.get("Numero de la voie"))
                                    .append("voie", line.get("Nom de la voie"))
                                    .append("lieuDit", line.get("Nom du lieu dit"))
                                    .append("codePostal", line.get("Code postal"))
                                    .append("commune", line.get("Nom de la commune"))
                    )
                    .append(
                            "location",
                            new BasicDBObject("type", "Point")
                                    .append(
                                            "coordinates",
                                            Arrays.asList(
                                                    Double.valueOf(line.get("Longitude")),
                                                    Double.valueOf(line.get("Latitude"))
                                            )
                                    )
                    )
                    .append("multiCommune", "Oui".equals(line.get("Multi commune")))
                    .append("nbPlacesParking", line.get("Nombre total de place de parking") == null || line.get("Nombre total de place de parking").isEmpty() ? 0 : Integer.valueOf(line.get("Nombre total de place de parking")))
                    .append("nbPlacesParkingHandicapes", line.get("Nombre total de place de parking handicapés") == null || line.get("Nombre total de place de parking handicapés").isEmpty() ? 0 : Integer.valueOf(line.get("Nombre total de place de parking handicapés")))
                    .append(
                            "dateMiseAJourFiche",
                            line.get("Date de mise à jour de la fiche installation") == null || line.get("Date de mise à jour de la fiche installation").isEmpty()
                                    ? null :
                                    Date.from(
                                            LocalDate.parse(line.get("Date de mise à jour de la fiche installation").substring(0, 10))
                                                    .atStartOfDay(ZoneId.of("UTC"))
                                                    .toInstant()
                                    )
                    );
            collection.insert(document);
        }
    }

    public static void remplissageEquipements(DBCollection collection){
        for (Map<String, String> line : ReadCVS.run(pathEquipements)) {
            collection.update(collection.findOne(line.get("InsNumeroInstall")),
                    new BasicDBObject("$push",new BasicDBObject("equipements", new BasicDBObject()
                            .append("numero", line.get("EquipementId"))
                            .append("nom", line.get("EquNom"))
                            .append("type", line.get("EquipementTypeLib"))
                            .append("famille", line.get("FamilleFicheLib")))));
        }
    }

    public static void remplissageActivites(DBCollection collection){
        for (Map<String, String> line : ReadCVS.run(pathActivites)) {
            BasicDBObject searchQuery = new BasicDBObject("equipements", new BasicDBObject("$elemMatch", new BasicDBObject("numero", line.get("Numéro de la fiche équipement").trim())));
            BasicDBObject updateQuery = new BasicDBObject("$push", new BasicDBObject("equipements.$.activites", line.get("Activité libellé")));
            collection.update(searchQuery, updateQuery);
        }
    }

}
