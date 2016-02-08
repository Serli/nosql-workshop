package nosql.workshop.model;

/**
 * Created by Théophile Morin & Remy Ferre
 */
public class Location {

    public Location() {
        coordinates = new Float[2];
    }

    public String type;
    public Float[] coordinates;
}
