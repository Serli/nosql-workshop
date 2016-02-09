package nosql.workshop.batch.mongodb;

import com.mongodb.*;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marion Bechennec
 */
public class ImportDataInMongo {

    private static final String DB_NAME = "nosql-workshop";
    private static final String COLLECTION_NAME = "installations";
    private final DBCollection collection;

    public ImportDataInMongo() {
        MongoClient client = new MongoClient();
        DB db = client.getDB(DB_NAME);
        collection = db.getCollection(COLLECTION_NAME);
    }

    private String cleanString(String s) {
        return s.matches("\".*\"")? s.substring(1, s.length() - 1): s;
    }

    private void insertInstallations() {
        InputStream is = getClass().getResourceAsStream("/batch/csv/installations.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        br.lines()
            .skip(1)
            .filter(line -> line.length() > 0)
            .map(line -> line.split(","))
            .forEach(column -> {
                //TODO insert in mongo
                DBObject installation = new BasicDBObject("type", "Installation")
                        .append("_id", cleanString(column[1]))
                        .append("nom", cleanString(column[0]))
                        .append("adresse", new BasicDBObject("type", "adresse")
                                .append("numero", cleanString(column[6]))
                                .append("voie", cleanString(column[7]))
                                .append("lieuDit", cleanString(column[5]))
                                .append("codePostal", cleanString(column[4]))
                                .append("commune", cleanString(column[2])))
                        .append("location", new BasicDBObject("type", "Point")
                                .append("coordinates", cleanString(column[8])))
                        .append("multiCommune", cleanString(column[16]))
                        .append("nbPlacesParking", cleanString(column[17]))
                        .append("nbPlacesParkingHandicapes", cleanString(column[18]))
                        .append("dateMiseAJourFiche", cleanString(column[28]));
                collection.save(installation);
            });
    }

    private void updateEquipements() {
        InputStream is = getClass().getResourceAsStream("/batch/csv/equipements.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        br.lines()
                .skip(1)
                .filter(line -> line.length() > 0)
                .map(line -> line.split(","))
                .forEach(column -> {
                    //TODO update in mongo
                    String numInstall = column[2];
                    String numero = column[4];
                    String nom = column[5];
                    String type = column[7];
                    String famille = column[9];

                    DBObject installation = collection.findOne(numInstall);
                    List<DBObject> equipements = (List<DBObject>) installation.get("equipements");
                    equipements.add(new BasicDBObject("numero", numero)
                            .append("nom", nom)
                            .append("type", type)
                            .append("famille", famille));

                    collection.update(new BasicDBObject("_id", numInstall),
                            new BasicDBObject("equipements", equipements));
                });
    }

    private void updateActivites() {
        InputStream is = getClass().getResourceAsStream("/batch/csv/activites.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        br.lines()
                .skip(1)
                .filter(line -> line.length() > 0)
                .map(line -> line.split(","))
                .forEach(column -> {
                    //TODO update in mongo
                    String numEquip = cleanString(column[2]);
                    DBObject installation = collection.findOne(new BasicDBObject("equipements.numero", numEquip));
                    installation.get("equipements");
                });
    }

    public static void main(String[] args) {
        //insert installations
        ImportDataInMongo importer = new ImportDataInMongo();

        importer.insertInstallations();
        //update installations with equipement
        importer.updateEquipements();
        //update equipement in insertInstallations with activites
        importer.updateActivites();

    }

}
