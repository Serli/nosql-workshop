package nosql.workshop.batch.mongodb;

import com.mongodb.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

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
        String value = s.matches("\".*\"") ? s.substring(1, s.length() - 1) : s;
        return value.trim();
    }

    private void insertInstallations() {
        InputStream is = getClass().getResourceAsStream("/batch/csv/installations.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        br.lines()
            .skip(1)
            .filter(line -> line.length() > 0)
            .map(line -> line.split("\",\""))
            .forEach(column -> {
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
                                .append("coordinates",
                                        Arrays.asList(Double.valueOf(cleanString(column[9])), Double.valueOf(cleanString(column[10])))))
                        .append("multiCommune", "Oui".equals(column[16]))
                        .append("nbPlacesParking", column[17].isEmpty() ? null : Integer.valueOf(column[17]))
                        .append("nbPlacesParkingHandicapes", column[18].isEmpty() ? null : Integer.valueOf(column[18]))
                        .append("dateMiseAJourFiche",
                            column.length < 29 || column[28].isEmpty() || column[28].length()<10 ? null : Date.from(
                                LocalDate.parse(column[28].substring(0, 10)).atStartOfDay(ZoneId.of("UTC"))
                                        .toInstant()));

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
                    String numInstall = column[2];
                    String numero = column[4];
                    String nom = column[5];
                    String type = column[7];
                    String famille = column[9];

                    BasicDBObject equipement = new BasicDBObject("numero", numero)
                            .append("nom", nom)
                            .append("type", type)
                            .append("famille", famille);

                    collection.update(new BasicDBObject("_id", numInstall),
                            new BasicDBObject("$addToSet", new BasicDBObject("equipements", equipement)));
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
                    String numEquip = cleanString(column[2]);
                    String nomActivite = cleanString(column[5]);

                    collection.update(new BasicDBObject("equipements.numero", numEquip),
                            new BasicDBObject("$addToSet",
                                    new BasicDBObject("equipements.$.activites", nomActivite)));
                });
    }

    public static void main(String[] args) {
        ImportDataInMongo importer = new ImportDataInMongo();

        //insert installations
        importer.insertInstallations();
        //update installations with equipement
        importer.updateEquipements();
        //update equipement in insertInstallations with activites
        importer.updateActivites();

    }

}
