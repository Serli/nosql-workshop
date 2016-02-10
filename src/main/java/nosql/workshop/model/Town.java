package nosql.workshop.model;

import io.searchbox.annotations.JestId;

import java.util.List;

/**
 * Created by Théophile Morin & Remy Ferre
 */
public class Town {

    @JestId
    private String id;
    private String name;
    private String sugggest;
    private String postCode;
    private String pays;
    private String region;
    private List<Float> location;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSugggest() {
        return sugggest;
    }

    public void setSugggest(String sugggest) {
        this.sugggest = sugggest;
    }

    public String getPostCode() {
        return postCode;
    }

    public void setPostCode(String postCode) {
        this.postCode = postCode;
    }

    public String getPays() {
        return pays;
    }

    public void setPays(String pays) {
        this.pays = pays;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<Float> getLocation() {
        return location;
    }

    public void setLocation(List<Float> location) {
        this.location = location;
    }
}
