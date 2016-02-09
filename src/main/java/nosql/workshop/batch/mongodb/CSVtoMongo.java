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
                        line.size() < 29 || line.get("Date de mise à jour de la fiche installation").isEmpty()
                                ? null :
                                Date.from(
                                        LocalDate.parse(line.get("Date de mise à jour de la fiche installation").substring(0, 10))
                                                .atStartOfDay(ZoneId.of("UTC"))
                                                .toInstant()
                                )
                );
        collection.insert(document);
    }

       /* for (Map<String, String> line : ReadCVS.run(pathEquipements)) {
            db.getCollection("people").update(new BasicDBObject()
                    .append("_id", line.get("EquipementId"))
                    .append("nom", line.get("EquNom"))
                    .append("type", line.get("EquipementTypeLib"))
                    .append("famille", line.get("FamilleFicheLib")), db.getCollection("people").findOne());
        }*/

       /* for (Map<String, String> line : ReadCVS.run(pathActivites)) {
            db.getCollection("people").update(new BasicDBObject()
                    .append("_id", line.get("Activité code"))
                    .append("nom", line.get("Activité libellé")), db.getCollection("people").findOne());
        }*/

    }





}
