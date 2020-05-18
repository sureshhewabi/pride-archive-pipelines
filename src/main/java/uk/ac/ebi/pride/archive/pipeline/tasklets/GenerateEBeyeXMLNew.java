package uk.ac.ebi.pride.archive.pipeline.tasklets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.ddi.xml.validator.parser.marshaller.OmicsDataMarshaller;
import uk.ac.ebi.ddi.xml.validator.parser.model.*;
import uk.ac.ebi.pride.archive.dataprovider.user.Contact;
import uk.ac.ebi.pride.data.model.CvParam;
import uk.ac.ebi.pride.data.model.Submission;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

public class GenerateEBeyeXMLNew {

    private static final Logger logger = LoggerFactory.getLogger(GenerateEBeyeXMLNew.class);
    private static final String NOT_AVAILABLE = "Not available";
    private static final String OMICS_TYPE = "Proteomics";
    private static final String PRIDE_URL = "http://www.ebi.ac.uk/pride/archive/projects/";
    private static final String DEFAULT_EXPERIMENT_TYPE = "Mass Spectrometry";
    private MongoPrideProject project;
    private File outputDirectory;
    private Submission submission;
    private HashMap<String, String> proteins;
    private boolean fromPride;

    private OmicsDataMarshaller omicsDataMarshaller;

    public GenerateEBeyeXMLNew(MongoPrideProject project, Submission submission, File outputDirectory, HashMap<String, String> proteins, boolean fromPride) {
        this.project = project;
        this.outputDirectory = outputDirectory;
        this.submission = submission;
        this.proteins = proteins;
        this.fromPride = fromPride;
    }

    public void generate() {
        if (this.project != null && this.submission != null && this.outputDirectory != null) {
            if (!this.project.isPublicProject()) {
                logger.error("Project " + this.project.getAccession() + " is still private, not generating EB-eye XML.");
            } else {
                try {

                    Database database = new Database();
                    database.setName("Pride Archive");
                    database.setDescription("");
                    database.setRelease("3");
                    database.setReleaseDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
                    database.setEntryCount(1);

                    Entries entries = new Entries();
                    Entry entry = new Entry();
                    entry.setId(project.getAccession());
                    entry.setName(project.getTitle());
                    String projectDescription = project.getDescription();
                    entry.setDescription((projectDescription != null && !projectDescription.isEmpty()) ? projectDescription : project.getTitle());
                    CrossReferences crossReferences = new CrossReferences();
                    crossReferences.setRef(getReferences());
                    entry.setCrossReferences(crossReferences);
                    entry.setDates(getDates());
                    uk.ac.ebi.ddi.xml.validator.parser.model.Date date = new uk.ac.ebi.ddi.xml.validator.parser.model.Date();

                    String databaseString = omicsDataMarshaller.marshall(database);

                }
            }
        }
    }

    private DatesType getDates() {


    }

    private List<Reference> getReferences() {
        List<Reference> references = new ArrayList<>();

        Collection<uk.ac.ebi.pride.archive.repo.repos.project.Reference> projectReferences = project.getReferences();
        if (projectReferences != null) {
            for (uk.ac.ebi.pride.archive.repo.repos.project.Reference reference :) {
                Reference xmlReference = new Reference();
                xmlReference.setDbkey(Integer.toString(reference.getPubmedId()));
                xmlReference.setDbname("pubmed");
                references.add(xmlReference);
            }
        }

        Set<CvParam> specieses = submission.getProjectMetaData().getSpecies();
        if (specieses != null) {
            for (CvParam species : specieses) {
                Reference xmlReference = new Reference();
                xmlReference.setDbkey(species.getAccession());
                xmlReference.setDbname("TAXONOMY");
                references.add(xmlReference);
            }
        }

        if (proteins != null) {
            for (String protein : proteins.keySet()) {
                Reference xmlReference = new Reference();
                xmlReference.setDbkey(protein);
                xmlReference.setDbname(proteins.get(protein));
                references.add(xmlReference);
            }
        }

        Collection users = project.getSubmittersContacts();

        if (users != null) {

            for (Object contact : users) {
                Reference xmlReference = new Reference();
                xmlReference.setDbkey(((Contact) contact).getOrcid());
                xmlReference.setDbname("ORCID");
                references.add(xmlReference);
            }

            return references;
        }
    }
