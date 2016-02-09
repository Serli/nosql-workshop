package nosql.workshop.batch.mongodb;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Created by chriswoodrow on 09/02/2016.
 */
public class CsvToMongoDb {

    public static String getColumnValue(String column) {
        return column.matches("\".*\"")?column.substring(1,column.length()-1):column;
    }

    private static int parseIntV2(String integer) {
        return integer.isEmpty() ? 0 : Integer.parseInt(integer);
    }

    private static boolean parseBooleanV2(String bool) {
        return bool.equals("Oui") ? true : false;
    }

    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        MongoDatabase db = mongoClient.getDatabase( "nosql-workshop" );


        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                        List<Float> coord = new ArrayList<Float>();
                        coord.add( Float.parseFloat(getColumnValue(columns[10])));
                        coord.add( Float.parseFloat(getColumnValue(columns[9])));
                            db.getCollection("installations").insertOne(new Document()
                                    .append("_id",getColumnValue(columns[1]))
                                    .append("nom",getColumnValue(columns[0]))
                                    .append("adresse",new Document()
                                            .append("numero",getColumnValue(columns[6]))
                                            .append("voie",getColumnValue(columns[7]))
                                            .append("lieuDit",getColumnValue(columns[5]))
                                            .append("codePostal",getColumnValue(columns[4]))
                                            .append("commune",getColumnValue(columns[8])))
                                    .append("location",new Document()
                                                    .append("type",getColumnValue("Point"))
                                                    .append("coordinates",coord))
                                    .append("multiCommune",getColumnValue(columns[16]).equals("Oui"))
                                    .append("nbPlacesParking",parseIntV2(getColumnValue(columns[17])))
                                    .append("nbPlacesParkingHandicapes",parseIntV2(getColumnValue(columns[18])))
                                    .append("DateMiseAjourFiche",getColumnValue(columns[28])));

                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
