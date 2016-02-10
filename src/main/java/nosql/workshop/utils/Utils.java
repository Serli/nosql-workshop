package nosql.workshop.utils;

/**
 * @author Killian
 */
public abstract class Utils {

    private Utils() {}

    /**
     * Removes double quotes from a String.
     * @param toClean the string ot clean
     * @return the cleaned string
     */
    public static String cleanString(String toClean) {
        return toClean.matches("\".*\"") ? toClean.substring(1, toClean.length() - 1).trim() : toClean.trim();
    }

    public static int getIntValue(String toClean) {
        String val = cleanString(toClean);
        return val.isEmpty() ? 0 : Integer.parseInt(val);
    }
}
