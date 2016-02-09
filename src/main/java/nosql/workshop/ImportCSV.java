package nosql.workshop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ImportCSV {
	public static void main(String[] args) {
		DB db = new MongoClient().getDB("dbname");

		Jongo jongo = new Jongo(db);
		MongoCollection installations = jongo.getCollection("installations");
		
		String csvFile = "src/main/resources/batch/csv/activites.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			br = new BufferedReader(new FileReader(csvFile));
			
			if ((line = br.readLine()) != null) {
				//String[] headers = line.split(cvsSplitBy);
			}
			DBObject inst = new BasicDBObject();
			while ((line = br.readLine()) != null) {

			        // use comma as separator
				String[] data = line.split(cvsSplitBy);
				//Nom usuel de l'installation0,"NumÃ©ro de l'installation"1,"Nom de la commune"2,"Code INSEE"3,"Code postal"4,"Nom du lieu dit"5,"Numero de la voie"6,"Nom de la voie"7,"location"8,"Longitude"9,"Latitude"10,"Aucun amÃ©nagement d'accessibilitÃ©"11,"AccessibilitÃ© handicapÃ©s Ã  mobilitÃ© rÃ©duite"12,"AccessibilitÃ© handicapÃ©s sensoriels"13,"Emprise fonciÃ¨re en m2"14,"GardiennÃ©e avec ou sans logement de gardien"15,"Multi commune"16,"Nombre total de place de parking"17,"Nombre total de place de parking handicapÃ©s"18,"Installation particuliÃ¨re"19,"Desserte mÃ©tro"19,"Desserte bus"20,"Desserte Tram"21,"Desserte train"22,"Desserte bateau"23,"Desserte autre"24,"Nombre total d'Ã©quipements sportifs"25,"Nombre total de fiches Ã©quipements"26,"Date de mise Ã  jour de la fiche installation"27
				inst = new BasicDBObject()
						.append("_id", data[1])
						.append("nom", data[0])
						.append("adresse", new BasicDBObject()
								.append("numero", data[6])
								.append("voie", data[7])
								.append("lieuDit", data[5])
								.append("codePostal", data[4])
								.append("commune", data[2])
								)
						.append("location", new BasicDBObject()
								.append("type", "point")
								.append("coordinates", 
										(Double.valueOf(data[9]) instanceof Double && Double.valueOf(data[10]) instanceof Double) 
										? Arrays.asList(Double.valueOf(data[9]), Double.valueOf(data[10]))
										: null)
								)
						.append("multiCommune", "Oui".equals(data[16]))
						.append("nbPlacesParking", data[17].isEmpty() ? null : Integer.valueOf(data[17]))
						.append("nbPlacesParkingHandicapes", data[18].isEmpty() ? null : Integer.valueOf(data[18]))
						.append(
								"dateMiseAJourFiche",
								data.length < 29 || data[28].isEmpty()
								? null :
									Date.from(
											LocalDate.parse(data[28].substring(0, 10))
											.atStartOfDay(ZoneId.of("UTC"))
											.toInstant()
											)
								);
			}
			
			installations.insert(inst);
			
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
	  }

	
}
