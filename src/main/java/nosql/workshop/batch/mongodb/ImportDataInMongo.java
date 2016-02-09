package nosql.workshop.batch.mongodb;

import com.mongodb.*;

import java.io.*;

/**
 * @author Marion Bechennec
 */
public class ImportDataInMongo {

    private static final String DB_NAME = "nosql-workshop";

    public static void main(String[] args) {
        //insert installations
        ImportDataInMongo importer = new ImportDataInMongo();

        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(DB_NAME);

        importer.insertInstallations(db);
        //update installations with equipement
        importer.updateEquipements();
        //update equipement in insertInstallations with activites
        importer.updateActivites();

    }

    private void insertInstallations(DB db) {
        DBCollection col = db.getCollection("installations");

        InputStream is = getClass().getResourceAsStream("installations.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        br.lines()
            .skip(1)
            .filter(line -> line.length() > 0)
            .map(line -> line.split(","))
            .forEach(column -> {
                //TODO insert in mongo
                DBObject installation = new BasicDBObject("type", "Installation")
                        .append("_id", column[1])
                        .append("nom", column[0])
                        .append("adresse", new BasicDBObject("type", "adresse")
                                .append("numero", column[6])
                                .append("voie", column[7])
                                .append("lieuDit", column[5])
                                .append("codePostal", column[4])
                                .append("commune", column[2]))
                        .append("location", new BasicDBObject("type", "Point")
                                .append("coordinates", column[8]))
                        .append("multiCommune", column[16])
                        .append("nbPlacesParking", column[17])
                        .append("nbPlacesParkingHandicapes", column[18])
                        .append("dateMiseAJourFiche", column[28]);
                col.insert(installation);
            });
    }

    private void updateEquipements() {
    }

    private void updateActivites() {
    }

}
