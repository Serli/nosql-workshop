package nosql.workshop.batch.mongodb;

import java.io.*;

/**
 * @author Marion Bechennec
 */
public class ImportDataInMongo {

    public static void main(String[] args) {
        //insert installations
        ImportDataInMongo importer = new ImportDataInMongo();

        importer.insertInstallations();
        //update installations with equipement
        importer.updateEquipements();
        //update equipement in insertInstallations with activites
        importer.updateActivites();

    }

    private void insertInstallations() {
        InputStream is = getClass().getResourceAsStream("installations.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        br.lines()
            .skip(1)
            .filter(line -> line.length() > 0)
            .map(line -> line.split(","))
            .forEach(column -> {
                //TODO insert in mongo
            });
    }

    private void updateEquipements() {
    }

    private void updateActivites() {
    }
}
