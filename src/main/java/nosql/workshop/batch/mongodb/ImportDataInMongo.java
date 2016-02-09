package nosql.workshop.batch.mongodb;

import com.google.common.collect.Sets;
import com.mongodb.*;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
                                        Arrays.asList(Double.valueOf(cleanString(column[10])), Double.valueOf(cleanString(column[9])))))
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
                    String numInstall = column[2];
                    String numero = column[4];
                    String nom = column[5];
                    String type = column[7];
                    String famille = column[9];

                    DBObject installation = collection.findOne(numInstall);
                    Set<DBObject> equipements;
                    BasicDBObject equipement = new BasicDBObject("numero", numero)
                            .append("nom", nom)
                            .append("type", type)
                            .append("famille", famille);
                    if (installation.containsField("equipements")) {
                        equipements = new HashSet<>((List<DBObject>) installation.get("equipements"));
                        equipements.add(equipement);
                    } else {
                        equipements = Sets.newHashSet(equipement);
                    }
                    installation.put("equipements", equipements);
                    collection.update(new BasicDBObject("_id", numInstall), installation);
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
