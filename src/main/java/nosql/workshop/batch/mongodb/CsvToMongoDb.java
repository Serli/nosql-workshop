package nosql.workshop.batch.mongodb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

import com.mongodb.*;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class CsvToMongoDb {

	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDB("nosql-workshop");

		DBCollection installations = db.getCollection("installations");

		importInstallation(installations);
		importEquipement(installations);
		importActivite(installations);

		BasicDBObject indexObj1 = new BasicDBObject()
				.append("nom", "text")
				.append("adresse.commune", "text");
		BasicDBObject indexObj2 = new BasicDBObject()
				.append("weights", new BasicDBObject()
						.append("nom", 3)
						.append("adresse.commune", 10)
						)
				.append("adresse.default_language", "french");
		installations.createIndex(indexObj1, indexObj2);
		installations.createIndex(new BasicDBObject("location", "2dsphere"));

	}

	public static void importInstallation(DBCollection installations) {


		String csvFile = "src/main/resources/batch/csv/installations.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\"";

		try {

			br = new BufferedReader(new FileReader(csvFile));

			if ((line = br.readLine()) != null) {
				//String[] headers = line.split(cvsSplitBy);
			}
			DBObject inst = new BasicDBObject();
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] data = line.split(cvsSplitBy);
				//System.out.println(data.length);
				//Nom usuel de l'installation0,"NumÃ©ro de l'installation"1,"Nom de la commune"2,"Code INSEE"3,"Code postal"4,"Nom du lieu dit"5,"Numero de la voie"6,"Nom de la voie"7,"location"8,"Longitude"9,"Latitude"10,"Aucun amÃ©nagement d'accessibilitÃ©"11,"AccessibilitÃ© handicapÃ©s Ã  mobilitÃ© rÃ©duite"12,"AccessibilitÃ© handicapÃ©s sensoriels"13,"Emprise fonciÃ¨re en m2"14,"GardiennÃ©e avec ou sans logement de gardien"15,"Multi commune"16,"Nombre total de place de parking"17,"Nombre total de place de parking handicapÃ©s"18,"Installation particuliÃ¨re"19,"Desserte mÃ©tro"19,"Desserte bus"20,"Desserte Tram"21,"Desserte train"22,"Desserte bateau"23,"Desserte autre"24,"Nombre total d'Ã©quipements sportifs"25,"Nombre total de fiches Ã©quipements"26,"Date de mise Ã  jour de la fiche installation"27
				inst = new BasicDBObject()
						.append("_id", data[3])
						.append("nom", data[1])
						.append("adresse", new BasicDBObject()
								.append("numero", data[13])
								.append("voie", data[15])
								.append("lieuDit", data[11])
								.append("codePostal", data[9])
								.append("commune", data[5])
								)
						.append("location", new BasicDBObject()
								.append("type", "Point")
								.append("coordinates",  Arrays.asList(Double.parseDouble(data[19]), Double.parseDouble(data[21])))
								)
						.append("multiCommune", "Oui".equals((data[35])))
						.append("nbPlacesParking", data[35].isEmpty() ? null : Integer.valueOf(data[35]))
						.append("nbPlacesParkingHandicapes", data[37].isEmpty() ? null : Integer.valueOf(data[37]))
						.append(
								"dateMiseAJourFiche",
								data.length < 58 || data[57].isEmpty()
								? null :
									Date.from(
											LocalDate.parse(data[57].substring(0, 10))
											.atStartOfDay(ZoneId.of("UTC"))
											.toInstant()
											)
								);

				installations.insert(inst);


				//installations.update(query, update)

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
	}


	public static void importEquipement(DBCollection installations) {
		String csvFile = "src/main/resources/batch/csv/equipements.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			br = new BufferedReader(new FileReader(csvFile));

			if ((line = br.readLine()) != null) {
				//String[] headers = line.split(cvsSplitBy);
			}
			DBObject equipement = new BasicDBObject();

			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] data = line.split(cvsSplitBy);

				equipement = new BasicDBObject()
						.append("numero", data[4]) // EquipemementID
						.append("nom", data[5]) // EquNom
						.append("type", data[7]) // EquipemementTypeLib
						.append("famille", data[9]); // FamilleFicheLib

				installations.update(new BasicDBObject("_id", data[2]), new BasicDBObject("$push", new BasicDBObject("equipements", equipement)));
				/*DBCursor cursor = installations.find(new BasicDBObject().append("_id", data[2]));
				try {
					while(cursor.hasNext()) {
						DBObject obj = cursor.next();
						installations.update(obj, new BasicDBObject("$push", new BasicDBObject("equipements", equipement)));
					}
				} finally {
					cursor.close();
				}*/
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
	}

	public static void importActivite(DBCollection installations) {

		String csvFile = "src/main/resources/batch/csv/activites.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\",\"";

		try {

			br = new BufferedReader(new FileReader(csvFile));

			if ((line = br.readLine()) != null) {
				//String[] headers = line.split(cvsSplitBy);
			}

			while ((line = br.readLine()) != null) {
				line = line.substring(1, line.length() - 1);
				String[] data = line.split(cvsSplitBy);

				BasicDBObject searchQuery = new BasicDBObject(
						"equipements",
						new BasicDBObject(
								"$elemMatch",
								new BasicDBObject("numero", getDataOrElse(data, 2, "").replace(" ", ""))
								)
						);

				BasicDBObject updateQuery = new BasicDBObject(
						"$push",
						new BasicDBObject("equipements.$.activites", getDataOrElse(data, 5, ""))
				);
				installations.update(searchQuery, updateQuery);
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
	}

	private static String getDataOrElse(String[] data, int index, String or){
		return index < data.length ? data[index] : or;
	}

}
