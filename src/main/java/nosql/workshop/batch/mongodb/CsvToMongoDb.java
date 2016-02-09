package nosql.workshop.batch.mongodb;

import java.io.*;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Created by chriswoodrow on 09/02/2016.
 */
public class CsvToMongoDb {
    public static void main(String[] args) {

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        System.out.println("Une ligne");
                        System.out.println(columns[0].matches("\".*\"")?columns[0].substring(1,columns[0].length()-1):columns[0]);
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
