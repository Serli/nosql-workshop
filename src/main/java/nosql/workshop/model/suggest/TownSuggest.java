package nosql.workshop.model.suggest;

import java.util.Arrays;
import java.util.List;

import org.jongo.marshall.jackson.oid.MongoId;

/**
 * Town suggestion
 */
public class TownSuggest {
	@MongoId
    public String _id;
    private String townName;
    private Double[] location;

    private TownSuggest() {
    }

    public void set_id(String s){
    	this._id=s;
    }
    
    public TownSuggest(String value, List<Double> location) {
        this.townName = value;
        this.location = location.toArray(new Double[location.size()]);
    }

    @Override
    public String toString() {
        return "TownSuggest{" +
                "townName='" + townName + '\'' +
                ", location=" + Arrays.toString(location) +
                '}';
    }

    public String getTownName() {
        return townName;
    }

    public void setTownName(String townName) {
        this.townName = townName;
    }

    public Double[] getLocation() {
        return location;
    }

    public void setLocation(Double[] location) {
        this.location = location;
    }
}
