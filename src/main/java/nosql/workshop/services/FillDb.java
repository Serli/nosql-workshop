package nosql.workshop.services;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class FillDb {
	
	public static void fillInstallations() throws IOException{
		List<String> lines = Files.readAllLines(Paths.get("src/main/resources/batch/csv/installations.csv"));
		
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
	
	public static void writeInstallations(List<String> lines){
		for(int i=1; i<lines.size(); i++){
			DBObject object = new BasicDBObject("type","installation")
							.append("name", "data");
		}				
	}
}
