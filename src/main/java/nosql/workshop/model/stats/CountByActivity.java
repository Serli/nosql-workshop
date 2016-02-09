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

    public void setTotal(long total) {
        this.total = total;
    }
}


/*

stats.setCountByActivity(Lists.newArrayList(installations.aggregate("{$unwind: \"$equipements\"}")
                .and("{$unwind: \"$equipements.activites\"}")
                .and("{$group: {_id: \"$equipements.activites\", total:{$sum : 1}}}")
                .and("{$project: {activite: \"$_id\", total : 1}}")
                .as(CountByActivity.class).iterator()));

*/