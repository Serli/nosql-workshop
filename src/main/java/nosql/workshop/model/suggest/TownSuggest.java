package nosql.workshop.model.suggest;

import java.util.Arrays;
import java.util.List;

/**
 * Town suggestion
 */
public class TownSuggest {
    private String townName;
    private Double[] location;

    public TownSuggest() {
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
