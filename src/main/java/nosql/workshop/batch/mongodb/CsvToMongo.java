package nosql.workshop.batch.mongodb;

import com.mongodb.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by hadri on 08/02/2016.
 */
public class CsvToMongo {

    public void activities() {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection col = db.getCollection("activites");


        String csvFile = "batch/csv/activites.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] country = line.split(cvsSplitBy);

                System.out.println("Country [code= " + country[4]
                        + " , name=" + country[5] + "]");

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



        DBObject obj = new BasicDBObject()
                .append("_id", columns[1])
                .append("nom", columns[0])
                .append("adresse",
                        new BasicDBObject()
                                .append("numero", columns[6])
                                .append("voie", columns[7])
                                .append("lieuDit", columns[5])
                                .append("codePostal", columns[4])
                                .append("commune", columns[2])
                )
                .append(
                        "location",
                        new BasicDBObject("type", "Point")
                                .append(
                                        "coordinates",
                                        Arrays.asList(
                                                Double.valueOf(columns[9]),
                                                Double.valueOf(columns[10])
                                        )
                                )
                )
                .append("multiCommune", "Oui".equals(columns[16]))
                .append("nbPlacesParking", columns[17].isEmpty() ? null : Integer.valueOf(columns[17]))
                .append("nbPlacesParkingHandicapes", columns[18].isEmpty() ? null : Integer.valueOf(columns[18]))
                .append(
                        "dateMiseAJourFiche",
                        columns.length < 29 || columns[28].isEmpty()
                                ? null :
                                Date.from(
                                        LocalDate.parse(columns[28].substring(0, 10))
                                                .atStartOfDay(ZoneId.of("UTC"))
                                                .toInstant()
                                )
                );


    }

}
