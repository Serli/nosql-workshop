package nosql.workshop.batch.mongodb;

import org.apache.commons.csv.CSVFormat;

import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * @author Marion Bechennec
 */
public class ImportDataInMongo {

    private static final String[] INSTALLATION_HEADER = {"Nom usuel de l'installation","Numéro de l'installation","Nom de la commune","Code INSEE","Code postal","Nom du lieu dit","Numero de la voie","Nom de la voie","location","Longitude","Latitude","Aucun aménagement d'accessibilité","Accessibilité handicapés à mobilité réduite","Accessibilité handicapés sensoriels","Emprise foncière en m2","Gardiennée avec ou sans logement de gardien","Multi commune","Nombre total de place de parking","Nombre total de place de parking handicapés","Installation particulière","Desserte métro","Desserte bus","Desserte Tram","Desserte train","Desserte bateau","Desserte autre","Nombre total d'équipements sportifs","Nombre total de fiches équipements","Date de mise à jour de la fiche installation"};

    public static void main(String[] args) {
        //insert installation
        ImportDataInMongo importer = new ImportDataInMongo();

        importer.installation();
        //update installation with equipement
        //update equipement in installation with activites

    }

    private void installation() {
        try {
            FileReader fileReader = new FileReader("activites.csv");
            CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(INSTALLATION_HEADER);



        } catch (FileNotFoundException e) {
        }
    }
}
