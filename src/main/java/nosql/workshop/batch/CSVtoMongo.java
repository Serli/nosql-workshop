package nosql.workshop.batch;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import nosql.workshop.model.Adresse;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Location;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Théophile Morin & Remy Ferre
 */
public class CSVtoMongo {


    public static void main(String[] args) throws FileNotFoundException, UnknownHostException{
        System.out.println("Status: Trying to connect to MongoDB...");

        long beg, end;
        MongoCollection installations = getCollection();
        List<Installation> installationList = readInstallationCSV();


        System.out.println("Status: Trying to insert data into db...");
        try {
            beg = System.currentTimeMillis();

            installations.drop();
            //installations.insert(installationList.toArray()); //Bulk insert

            end = System.currentTimeMillis();
            System.out.println("Status: data inserted in " + (end - beg)+ "ms.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        addEquipementDataFromCSV(installations);
        //TODO add activities;

        System.out.println("Job done.");
    }

    private static MongoCollection getCollection() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        MongoCollection installations = db.getCollection("installations");
        return installations;

        //Or with Jongo
        /*MongoDB db = new MongoDB();
        Jongo jongo = db.getJongo();
        MongoCollection installations = jongo.getCollection("installations");*/
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

        /*Installation current = collection.findOne("{_id : #}", ref).as(Installation.class);
        Equipement[] news = new Equipement[current.equipements.length+1];
        System.arraycopy(current.equipements,0, news, 0, current.equipements.length);
        news[current.equipements.length] = eq;
        current.equipements = news;

        collection.save(current);*/
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




    private static DBObject mapInstallationCSVtoDBObject(String[] csvData) {
        try {
            DBObject addr = new BasicDBObject("commune", csvData[2])
                    .append("codePostal", "".equals(csvData[4])? 0 : Integer.parseInt(csvData[4]))
                    .append("numero", csvData[6])
                    .append("voie", csvData[7])
                    .append("lieuDit",  csvData[5]);

            DBObject loc = new BasicDBObject("type", "Point")
                    .append("coordinates", Arrays.asList(Float.parseFloat(csvData[9]),Float.parseFloat(csvData[10])));

            DBObject inst = new BasicDBObject("_id", csvData[1])
                    .append("nom", csvData[0].substring(1))
                    .append("adresse", addr)
                    .append("multiCommune", "oui".equalsIgnoreCase(csvData[16]))
                    .append("nbPlacesParking", "".equals(csvData[17]) ? 0 : Integer.parseInt(csvData[17]))
                    .append("nbPlacesParkingHandicapes", "".equals(csvData[18]) ? 0 : Integer.parseInt(csvData[18]))
                    .append("dateMiseAJourFiche", csvData[28].substring(0, csvData[28].length() - 2))
                    .append("location", loc)
                    .append("equipements", Arrays.asList());

            return inst;
        }catch(Exception e) {
            System.err.println(e.toString());
            return null;
        }
    }


    private static DBObject mapEquipementCSVtoDBObject(String[] csvData) {
        try {
            int offset = 0;
            Integer numero = null;
            try {
                numero = Integer.parseInt(csvData[4]);
            }catch(Exception e) {
                System.err.println(e.toString());
                while(!csvData[4+offset].substring(csvData[4+offset].length()-1, csvData[4+offset].length()).equals("\"")) {
                    ++offset;
                    System.err.println("Retrying for: " + csvData[4+offset]);
                }
                ++offset;
                System.err.println("Retrying for: " + csvData[4+offset]);
                numero = Integer.parseInt(csvData[4+offset]);
            }
            DBObject equip = new BasicDBObject("numero", numero)
                    .append("nom", csvData[5+offset])
                    .append("type", csvData[7+offset])
                    .append("famille", csvData[9+offset])
                    .append("activites", Arrays.asList());

            return equip;
        }catch(Exception e) {
            System.err.println(e.toString());
            return null;
        }
    }

}
