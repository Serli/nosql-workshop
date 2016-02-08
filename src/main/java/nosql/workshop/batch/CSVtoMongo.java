package nosql.workshop.batch;

import nosql.workshop.model.Adresse;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Location;
import nosql.workshop.services.MongoDB;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Théophile Morin & Remy Ferre
 */
public class CSVtoMongo {


    public static void main(String[] args) throws FileNotFoundException, UnknownHostException{
        System.out.println("Status: Trying to connect to MongoDB...");

        //Retrieving Jongo instance to use MongoDB.
        MongoDB db = new MongoDB();
        long beg, end;
        Jongo jongo = db.getJongo();
        MongoCollection installations = jongo.getCollection("installations");

        List<Installation> installationList = readInstallationCSV();

        System.out.println("Status: Trying to insert data into db...");
        try {
            beg = System.currentTimeMillis();

            installations.drop();
            installations.insert(installationList.toArray()); //Bulk insert

            end = System.currentTimeMillis();
            System.out.println("Status: data inserted in " + (end - beg)+ "ms.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        addEquipementDataFromCSV(installations);
        //TODO add activities;

        System.out.println("Job done.");
    }

    private static void addEquipementDataFromCSV(MongoCollection collection)
            throws FileNotFoundException  {
        String path = "src/main/resources/batch/csv/equipements.csv";

        System.out.println("Status: Starting... ");
        System.out.println("Status: CSV file path -> " + path);


        System.out.println("Status: Start parsing...");
        long beg = System.currentTimeMillis();
        Scanner lineScanner = new Scanner(new File(path));
        lineScanner.useDelimiter("\n");

        if(lineScanner.hasNext()) //Skip first line.
            System.out.println(lineScanner.next());

        int counter=1;
        while(lineScanner.hasNext()) {
            String currentLine = lineScanner.next();
            addEquipementData(collection, currentLine);
            ++counter;
        }
        lineScanner.close();
        long end = System.currentTimeMillis();

        System.out.println("Status: " + counter + " equipment lines parsed from the CSV in " + (end - beg)+ "ms.");
    }



    private static List<Installation>readInstallationCSV() throws FileNotFoundException {
        String path = "src/main/resources/batch/csv/installations.csv";

        System.out.println("Status: Starting... ");
        System.out.println("Status: CSV file path -> " + path);

        List<Installation> installationList = new LinkedList<>();


        System.out.println("Status: Start parsing...");
        long beg = System.currentTimeMillis();
        Scanner lineScanner = new Scanner(new File(path));
        lineScanner.useDelimiter("\n");

        if(lineScanner.hasNext()) //Skip first line.
            lineScanner.next();

        while(lineScanner.hasNext()) {
            String currentLine = lineScanner.next();
            installationList.add(readInstallationLine(currentLine));
        }
        lineScanner.close();
        long end = System.currentTimeMillis();


        System.out.println("Status: " + installationList.size() + " lines parsed from the CSV in " + (end - beg)+ "ms.");
        return installationList;
    }

    private static Installation readInstallationLine(String csvLine) {
        Scanner scanner = new Scanner(csvLine);
        scanner.useDelimiter("\",\"");

        String[] instData = new String[29]; // 30 fields per line in csv.
        int counter = 0;

        Installation inst = null;
        try {
            while (scanner.hasNext()) {
                instData[counter] = scanner.next();
                ++counter;
            }
            inst = mapCSVwithPojo(instData);
        }catch(ArrayIndexOutOfBoundsException e) {
            System.err.println("Error while parsing line:");
            System.err.println(csvLine + "\n");
        }

        scanner.close();
        return inst;
    }





    private static void addEquipementData(MongoCollection collection, String csvLine) {
        Scanner scanner = new Scanner(csvLine);
        scanner.useDelimiter(",");

        String[] instData = new String[20]; // 183 fields per line in csv.
        int counter = 0;

        Equipement eq = null;
        try {
            while (scanner.hasNext()) {
                if(counter < 10)
                    instData[counter] = scanner.next();
                else
                    scanner.next();
                ++counter;
            }
            eq = mapCSVwithEquipmentPojo(instData);
        }catch(ArrayIndexOutOfBoundsException e) {
            System.err.println("Error while parsing line:");
            System.err.println(csvLine + "\n");
            System.err.println(e.toString());
        }

        scanner.close();

        String ref = instData[2];

        Installation current = collection.findOne("{_id : #}", ref).as(Installation.class);
        Equipement[] news = new Equipement[current.equipements.length+1];
        System.arraycopy(current.equipements,0, news, 0, current.equipements.length);
        news[current.equipements.length] = eq;
        current.equipements = news;

        collection.save(current);
        // or
        //collection.update("{_id : #}", ref).with(current);
    }


    private static Installation mapCSVwithPojo(String[] csvData) {
        Installation inst = null;
        if(csvData != null) {
            try {
                inst = new Installation(csvData[1]);
                inst.nom = csvData[0].substring(1);
                inst.multiCommune = "oui".equalsIgnoreCase(csvData[16]);
                inst.nbPlacesParking = "".equals(csvData[17]) ? 0 : Integer.parseInt(csvData[17]);
                inst.nbPlacesParkingHandicapes = "".equals(csvData[18]) ? 0 : Integer.parseInt(csvData[18]);
                inst.dateMiseAJourFiche = csvData[28].substring(0, csvData[28].length() - 2);

                Adresse addr = new Adresse();
                addr.commune = csvData[2];
                addr.codePostal = "".equals(csvData[4])? 0 : Integer.parseInt(csvData[4]); //Change to INSEE CODE csvData[3] ?
                addr.numero = csvData[6];
                addr.voie = csvData[7];
                addr.lieuDit = csvData[5];

                inst.adresse = addr;

                Location loc = new Location();
                loc.type = "Point";
                loc.coordinates[0] = Float.parseFloat(csvData[9]); //Longitude
                loc.coordinates[1] = Float.parseFloat(csvData[10]); //Latitude

                inst.location = loc;

                inst.equipements = new Equipement[0];
            }catch(Exception e) {
                System.err.println(e.toString());
            }
        }
        return inst;
    }


    private static Equipement mapCSVwithEquipmentPojo(String[] csvData) {
        Equipement eq = null;

        if(csvData != null) {
            try {
                int offset = 0;
                try {
                    eq = new Equipement(Integer.parseInt(csvData[4]));
                }catch(Exception e) {
                    System.err.println(e.toString());
                    while(!csvData[4+offset].substring(csvData[4+offset].length()-1, csvData[4+offset].length()).equals("\"")) {
                        ++offset;
                        System.err.println("Retrying for: " + csvData[4+offset]);
                    }
                    ++offset;
                    System.err.println("Retrying for: " + csvData[4+offset]);
                    eq = new Equipement(Integer.parseInt(csvData[4+offset]));
                }
                eq.nom = csvData[5+offset];
                eq.type = csvData[7+offset];
                eq.famille = csvData[9+offset];

                eq.activites = new String[0];
            }catch(Exception e) {
                System.err.println(e.toString());
            }
        }
        return eq;
    }
}
