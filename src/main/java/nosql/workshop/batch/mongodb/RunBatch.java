package nosql.workshop.batch.mongodb;

/**
 * Created by romanlp on 10/02/16.
 */
public class RunBatch {

    /* Lancer ce main pour réaliser l'importation des données des CSV
     * dans les bases de données MongoDB et ElasticSearch
     *
     * @author Antoine BOVIN - Roman LAPACHERIE
     */
    public static void main(String[] args) {

        CsvToMongoDb mongo = new CsvToMongoDb();

        mongo.run();

        MongoToElastic elastic = new MongoToElastic();

        elastic.run();

    }
}
