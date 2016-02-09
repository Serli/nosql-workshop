package nosql.workshop.batch.mongodb; /*
 * ${FILE_NAME}
 * author:   Maxime Perocheau
 * created:  2016 février 08 @ 17:25
 * modified: 2016 février 08 @ 17:25
 *
 * TODO : description
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class ReadCVS {

    public static List<Map<String, String>> run(String path) {

        String csvFile = path;
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = "\",\"";
        List<Map<String, String>> result = new ArrayList<>();
        List<String> head = new ArrayList<>();

        try {

            br = new BufferedReader(new FileReader(csvFile));

            boolean firstLine = true;

            while ((line = br.readLine()) != null) {

                if (firstLine){
                    head = Arrays.asList(line.split(cvsSplitBy));
                    firstLine = false;
                } else {

                    List<String> myLine = Arrays.asList(line.split(cvsSplitBy));
                    Map<String, String> map = new HashMap<>();

                    // use comma as separator
                    for (String e : myLine){
                        int index = myLine.indexOf(e);
                        map.put(head.get(index).replace("\"", ""), e.replace("\"", ""));
                    }
                    result.add(map);
                }
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

        return result;
    }

}
