package uk.ac.ebi.pride.archive.pipeline.utility;

import java.util.Date;

public class DateUtils {
    public static boolean equalsDate(Date a, Date b) {
        return  ((a == b) || (a != null && b != null && a.getTime() == b.getTime()));
    }
}
