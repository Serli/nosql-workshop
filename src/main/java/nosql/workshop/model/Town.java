package nosql.workshop.model;

public class Town {
	
	private String objectID;
	private String townName;
	private String townNameSuggest;
	private String pays;
	private String postcode;
	private String region;
	private String x;
	private String y;
	public String getObjectID() {
		return objectID;
	}
	public void setObjectID(String objectID) {
		this.objectID = objectID;
	}
	public String getTownName() {
		return townName;
	}
	public void setTownName(String townName) {
		this.townName = townName;
	}
	public String getTownNameSuggest() {
		return townNameSuggest;
	}
	public void setTownNameSuggest(String townNameSuggest) {
		this.townNameSuggest = townNameSuggest;
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
	public String getX() {
		return x;
	}
	public void setX(String x) {
		this.x = x;
	}
	public String getY() {
		return y;
	}
	public void setY(String y) {
		this.y = y;
	}
	public String getPostcode() {
		return postcode;
	}
	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}

}
