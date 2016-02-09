package nosql.workshop.model.stats;

public class CountByActivity {

    private String activite;
    private long total;

    public String getActivite() {
        return activite;
    }

    public void setActivite(String activite) {
        this.activite = activite;
    }

    public long getTotal() {
        return total;
    }

    public CountByActivity increment(){
        this.total ++;
        return this;
    }

    @Override
    public boolean equals(Object o){
       return (o instanceof CountByActivity
       && ((CountByActivity)o).getActivite()==this.activite);
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
