package nosql.workshop.services;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class FillDb {
	
	public static void fillInstallations() throws IOException{
		Files.lines(Paths.get("src/main/resources/batch/csv/installations.csv")).forEach(
				x -> display(x)
				);
	}
	
	public static void fillEquipements() throws IOException{
		Files.lines(Paths.get("src/main/resources/batch/csv/equipements.csv")).forEach(x -> display(x));
	}
	
	public static void fillActivities() throws IOException{
		Files.lines(Paths.get("src/main/resources/batch/csv/activites.csv")).forEach(x -> display(x));
	}
	public static void main(String[] args) throws IOException {
		FillDb.fillInstallations();
	}
	
	public static void display(String s){
		System.out.println(s);
	}
	
	public static void writeInstallations( String csv){
		String[] data = csv.split(",");
		DBObject object = new BasicDBObject("type","installation")
							.append("name", data[0])
							.append("",data[1])
							.append("",data[2]);//TODO use a loop with the names given in the first csv line
					
	}
}
