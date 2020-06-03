package uk.ac.ebi.pride.archive.pipeline.tasklets;

import uk.ac.ebi.ddi.xml.validator.parser.model.Date;
import uk.ac.ebi.ddi.xml.validator.parser.model.DatesType;

import java.util.List;

public class PrideDateTypes extends DatesType {

    public void setDates(List<Date> dates) {
        super.date = dates;
    }
}
