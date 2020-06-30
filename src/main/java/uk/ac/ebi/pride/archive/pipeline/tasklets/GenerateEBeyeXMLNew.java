package uk.ac.ebi.pride.archive.pipeline.tasklets;

import lombok.extern.slf4j.Slf4j;
import uk.ac.ebi.ddi.xml.validator.parser.marshaller.OmicsDataMarshaller;
import uk.ac.ebi.ddi.xml.validator.parser.model.*;
import uk.ac.ebi.ddi.xml.validator.utils.OmicsType;
import uk.ac.ebi.pride.archive.dataprovider.user.Contact;
import uk.ac.ebi.pride.archive.dataprovider.user.ContactProvider;
import uk.ac.ebi.pride.data.model.CvParam;
import uk.ac.ebi.pride.data.model.DataFile;
import uk.ac.ebi.pride.data.model.Param;
import uk.ac.ebi.pride.data.model.Submission;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.stream.Collectors;

import static uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants.GenerateEbeyeXmlConstants.*;

@Slf4j
public class GenerateEBeyeXMLNew {

    private MongoPrideProject project;
    private File outputDirectory;
    private Submission submission;
    private Map<String, String> proteins;
    private boolean fromPride;

    private OmicsDataMarshaller omicsDataMarshaller;

    public GenerateEBeyeXMLNew(MongoPrideProject project, Submission submission, File outputDirectory, Map<String, String> proteins, boolean fromPride) {
        this.project = project;
        this.outputDirectory = outputDirectory;
        this.submission = submission;
        this.proteins = proteins;
        this.fromPride = fromPride;
        omicsDataMarshaller = new OmicsDataMarshaller();
    }

    public void generate() {
        if (this.project != null && this.submission != null && this.outputDirectory != null) {
            if (!this.project.isPublicProject()) {
                log.error("Project " + this.project.getAccession() + " is still private, not generating EB-eye XML.");
            } else {
                try (FileOutputStream outputFile = new FileOutputStream(new File(this.outputDirectory, PRIDE_EBEYE + this.project.getAccession() + XML))) {

                    Database database = new Database();
                    database.setName(PRIDE_DATABASE_NAME);
                    database.setDescription("");
                    database.setRelease("3");
                    database.setReleaseDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
                    database.setEntryCount(1);

                    Entries entries = new Entries();
                    Entry entry = new Entry();
                    entry.setId(project.getAccession());
                    entry.setName(removeNonPrintableChars(project.getTitle()));
                    String projectDescription = project.getDescription();
                    entry.setDescription((projectDescription != null && !projectDescription.isEmpty()) ? projectDescription : project.getTitle());
                    CrossReferences crossReferences = new CrossReferences();
                    crossReferences.setRef(getReferences());
                    entry.setCrossReferences(crossReferences);
                    PrideDateTypes prideDateTypes = new PrideDateTypes();

                    uk.ac.ebi.ddi.xml.validator.parser.model.Date publicationDate = new uk.ac.ebi.ddi.xml.validator.parser.model.Date();
                    publicationDate.setType(PUBLICATION);
                    publicationDate.setValue((new SimpleDateFormat(YYYY_MM_DD)).format(this.project.getPublicationDate()));

                    uk.ac.ebi.ddi.xml.validator.parser.model.Date submissionDate = new uk.ac.ebi.ddi.xml.validator.parser.model.Date();
                    submissionDate.setType(SUBMISSION);
                    submissionDate.setValue((new SimpleDateFormat(YYYY_MM_DD)).format(this.project.getSubmissionDate()));

                    prideDateTypes.setDates(Arrays.asList(publicationDate, submissionDate));
                    entry.setDates(prideDateTypes);

                    AdditionalFields additionalFields = new AdditionalFields();

                    List<Field> additionalFieldsList = new ArrayList<>();

                    Field omicsField = new Field();
                    omicsField.setName(OMICS_TYPE);
                    omicsField.setValue(OmicsType.PROTEOMICS.getName());

                    additionalFieldsList.add(omicsField);

                    Field fullDatasetLink = new Field();
                    fullDatasetLink.setName(FULL_DATASET_LINK);
                    fullDatasetLink.setValue(PRIDE_URL + project.getAccession());

                    additionalFieldsList.add(fullDatasetLink);

                    Field repository = new Field();
                    repository.setName(REPOSITORY);
                    repository.setValue(PRIDE);

                    additionalFieldsList.add(repository);

                    String sampleProcessingProtocol = this.project.getSampleProcessingProtocol();

                    if (sampleProcessingProtocol != null && !sampleProcessingProtocol.isEmpty()) {
                        Field sampleProtocol = new Field();
                        sampleProtocol.setName(SAMPLE_PROTOCOL);
                        sampleProtocol.setValue(removeNonPrintableChars(sampleProcessingProtocol));
                        additionalFieldsList.add(sampleProtocol);
                    }

                    String dataProcessingProtocol = this.project.getDataProcessingProtocol();

                    if (dataProcessingProtocol != null && !dataProcessingProtocol.isEmpty()) {
                        Field dataProtocol = new Field();
                        dataProtocol.setName(DATA_PROTOCOL);
                        dataProtocol.setValue(removeNonPrintableChars(dataProcessingProtocol));
                        additionalFieldsList.add(dataProtocol);
                    }

                    // Instruments
                    Set<CvParam> instruments = this.submission.getProjectMetaData().getInstruments();

                    if (instruments != null && instruments.size() > 0) {
                        Set<String> modificationNames = instruments.stream().map(Param::getName).collect(Collectors.toSet());
                        modificationNames.stream().forEach(modificationName ->
                        {
                            Field instrumentPlatform = new Field();
                            instrumentPlatform.setName(INSTRUMENT_PLATFORM);
                            instrumentPlatform.setValue(modificationName);
                            additionalFieldsList.add(instrumentPlatform);
                        });
                    } else {
                        Field instrumentPlatform = new Field();
                        instrumentPlatform.setName(INSTRUMENT_PLATFORM);
                        instrumentPlatform.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(instrumentPlatform);
                    }

                    //Species
                    Set<CvParam> species = this.submission.getProjectMetaData().getSpecies();

                    if (species != null && species.size() > 0) {
                        species.stream().forEach(speciesX ->
                        {
                            Field speciesField = new Field();
                            speciesField.setName(SPECIES);
                            speciesField.setValue(speciesX.getName());
                            additionalFieldsList.add(speciesField);
                        });
                    } else {
                        Field speciesField = new Field();
                        speciesField.setName(SPECIES);
                        speciesField.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(speciesField);
                    }

                    //Celltypes
                    Set<CvParam> cellTypes = this.submission.getProjectMetaData().getCellTypes();

                    if (cellTypes != null && cellTypes.size() > 0) {
                        cellTypes.stream().forEach(cellTypeX ->
                        {
                            Field cellTypeField = new Field();
                            cellTypeField.setName(CELL_TYPE);
                            cellTypeField.setValue(cellTypeX.getName());
                            additionalFieldsList.add(cellTypeField);
                        });
                    } else {
                        Field cellTypeField = new Field();
                        cellTypeField.setName(CELL_TYPE);
                        cellTypeField.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(cellTypeField);
                    }


                    //Diseases
                    Set<CvParam> diseases = this.submission.getProjectMetaData().getDiseases();

                    if (diseases != null && diseases.size() > 0) {
                        diseases.stream().forEach(diseaseX ->
                        {
                            Field diseaseField = new Field();
                            diseaseField.setName(DISEASE);
                            diseaseField.setValue(diseaseX.getName());
                            additionalFieldsList.add(diseaseField);
                        });
                    } else {
                        Field diseaseField = new Field();
                        diseaseField.setName(DISEASE);
                        diseaseField.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(diseaseField);
                    }


                    //Tissues
                    Set<CvParam> tissues = this.submission.getProjectMetaData().getTissues();

                    if (tissues != null && tissues.size() > 0) {
                        tissues.stream().forEach(tissue ->
                        {
                            Field tissueField = new Field();
                            tissueField.setName(TISSUE);
                            tissueField.setValue(tissue.getName());
                            additionalFieldsList.add(tissueField);
                        });
                    } else {
                        Field tissueField = new Field();
                        tissueField.setName(TISSUE);
                        tissueField.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(tissueField);
                    }


                    // PTMS
                    Collection<? extends String> ptms = this.project.getPtms();

                    if (ptms != null && ptms.size() > 0) {
                        ptms.stream().forEach(ptm ->
                        {
                            Field ptmField = new Field();
                            ptmField.setName(MODIFICATION);
                            ptmField.setValue(ptm);
                            additionalFieldsList.add(ptmField);
                        });
                    } else {
                        Field ptmField = new Field();
                        ptmField.setName(MODIFICATION);
                        ptmField.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(ptmField);
                    }

                    // ExperimentType
                    Set<CvParam> projectExperimentTypes = this.submission.getProjectMetaData()
                            .getMassSpecExperimentMethods();

                    if (projectExperimentTypes != null && projectExperimentTypes.size() > 0) {
                        projectExperimentTypes.stream().forEach(projectExperimentType ->
                        {
                            Field projectExperimentTypeField = new Field();
                            projectExperimentTypeField.setName(TECHNOLOGY_TYPE);
                            projectExperimentTypeField.setValue(projectExperimentType.getName());
                            additionalFieldsList.add(projectExperimentTypeField);
                        });
                    }

                    Field projectExperimentTypeField = new Field();
                    projectExperimentTypeField.setName(TECHNOLOGY_TYPE);
                    projectExperimentTypeField.setValue(DEFAULT_EXPERIMENT_TYPE);
                    additionalFieldsList.add(projectExperimentTypeField);


                    //ProjectTags
                    Collection<String> projectTags = this.project.getProjectTags();

                    if (projectTags != null && projectTags.size() > 0) {
                        projectTags.stream().forEach(projectTag ->
                        {
                            Field projectTagField = new Field();
                            projectTagField.setName(CURATOR_KEYWORDS);
                            projectTagField.setValue(projectTag);
                            additionalFieldsList.add(projectTagField);
                        });
                    }


                    //ProjectKeywords
                    Collection<String> projectKeywords = this.project.getKeywords();

                    if (projectKeywords != null && projectKeywords.size() > 0) {
                        projectKeywords.stream().forEach(projectKeyword ->
                        {
                            Field projectKeywordField = new Field();
                            projectKeywordField.setName(SUBMITTER_KEYWORDS);
                            projectKeywordField.setValue(projectKeyword.trim());
                            additionalFieldsList.add(projectKeywordField);
                        });
                    }

                    //QuantificationMethods
                    Collection<String> quantificationMethods = this.project.getQuantificationMethods();

                    if (quantificationMethods != null && quantificationMethods.size() > 0) {
                        quantificationMethods.stream().forEach(quantificationMethod ->
                        {
                            Field quantificationMethodField = new Field();
                            quantificationMethodField.setName(QUANTIFICATION_METHOD);
                            quantificationMethodField.setValue(quantificationMethod);
                            additionalFieldsList.add(quantificationMethodField);
                        });
                    } else {
                        Field quantificationMethodField = new Field();
                        quantificationMethodField.setName(QUANTIFICATION_METHOD);
                        quantificationMethodField.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(quantificationMethodField);
                    }

                    //SubmissionType
                    Field submissionType = new Field();
                    submissionType.setName(SUBMISSION_TYPE);
                    submissionType.setValue(project.getSubmissionType());
                    additionalFieldsList.add(submissionType);


                    //QuantificationMethods
                    Collection<? extends String> softwares = this.project.getSoftwares();

                    if (softwares != null && softwares.size() > 0) {
                        softwares.stream().forEach(software ->
                        {
                            Field softwareField = new Field();
                            softwareField.setName(SOFTWARE);
                            softwareField.setValue(software);
                            additionalFieldsList.add(softwareField);
                        });
                    } else {
                        Field softwareField = new Field();
                        softwareField.setName(SOFTWARE);
                        softwareField.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(softwareField);
                    }


                    //Doi
                    Optional<String> doi = this.project.getDoi();

                    if (doi != null && doi.isPresent() && !doi.get().isEmpty()) {
                        Field doiField = new Field();
                        doiField.setName(DOI);
                        doiField.setValue(doi.get());
                        additionalFieldsList.add(doiField);
                    }


                    //References
                    Collection<uk.ac.ebi.pride.archive.dataprovider.reference.Reference> references = this.project.getReferencesWithPubmed();

                    if (references != null && references.size() > 0) {
                        references.stream().forEach(reference ->
                        {
                            Field referenceField = new Field();
                            referenceField.setName(PUBLICATION);
                            String url = "";

                            if (reference.getPubmedId() != 0) {
                                url = reference.getPubmedId() + "";
                            }

                            if (reference.getReferenceLine() != null && !reference.getReferenceLine().isEmpty()) {
                                url = url + " " + reference.getReferenceLine();
                            }

                            if (reference.getDoi() != null && !reference.getDoi().isEmpty()) {
                                url = url + " " + reference.getDoi();
                            }

                            referenceField.setValue(url.trim());
                            additionalFieldsList.add(referenceField);
                        });
                    } else {
                        Field softwareField = new Field();
                        softwareField.setName(PUBLICATION);
                        softwareField.setValue(NOT_AVAILABLE);
                        additionalFieldsList.add(softwareField);
                    }


                    //Submitter
                    Collection<? extends ContactProvider> submitters = this.project.getSubmittersContacts();

                    if (submitters != null) {
                        submitters.stream().forEach(submitter -> {
                            Field submitterField = new Field();
                            submitterField.setName(SUBMITTER);
                            submitterField.setValue(submitter.getName());
                            additionalFieldsList.add(submitterField);

                            Field submitterMailField = new Field();
                            submitterMailField.setName(SUBMITTER_MAIL);
                            submitterMailField.setValue(submitter.getEmail());
                            additionalFieldsList.add(submitterMailField);

                            String affiliation = submitter.getAffiliation();

                            if (affiliation != null && !affiliation.isEmpty()) {
                                Field affiliationField = new Field();
                                affiliationField.setName(SUBMITTER_AFFILIATION);
                                affiliationField.setValue(affiliation);
                                additionalFieldsList.add(affiliationField);
                            }

                            String country = submitter.getCountry();
                            if (country != null && !country.isEmpty()) {
                                Field countryField = new Field();
                                countryField.setName(SUBMITTER_COUNTRY);
                                countryField.setValue(country);
                                additionalFieldsList.add(countryField);
                            }
                        });

                    }

                    //Labheads
                    Collection<Contact> labHeads = this.project.getHeadLabContacts();

                    if (labHeads != null && !labHeads.isEmpty()) {
                        labHeads.stream().forEach(labhead -> {
                            Field labheadNameField = new Field();
                            labheadNameField.setName(LABHEAD);
                            labheadNameField.setValue(labhead.getName());
                            additionalFieldsList.add(labheadNameField);

                            Field labheadMailField = new Field();
                            labheadMailField.setName(LABHEAD_MAIL);
                            labheadMailField.setValue(labhead.getEmail());
                            additionalFieldsList.add(labheadMailField);

                            if (labhead.getAffiliation() != null) {
                                Field affiliationField = new Field();
                                affiliationField.setName(LABHEAD_AFFILIATION);
                                affiliationField.setValue(labhead.getAffiliation());
                                additionalFieldsList.add(affiliationField);
                            }
                        });
                    }

                    //DataFiles
                    List<DataFile> dataFiles = this.submission.getDataFiles();

                    if (dataFiles != null && !dataFiles.isEmpty()) {
                        dataFiles.stream().forEach(dataFile -> {
                            Field datafileField = new Field();
                            datafileField.setName(DATASET_FILE);
                            String url = "";
                            if (this.fromPride) {
                                Date pubDate = this.project.getPublicationDate();
                                Calendar calendar = Calendar.getInstance();
                                calendar.setTime(pubDate);
                                int month = calendar.get(2) + 1;
                                int year = calendar.get(1);
                                url = PRIDE_FTP_URL + year + "/" + (month < 10 ? "0" : "") + month +
                                        "/" + this.project.getAccession() + "/" + dataFile.getFileName();

                            } else if (dataFile.getUrl() != null && !dataFile.getUrl().toString().isEmpty()) {
                                url = dataFile.getUrl().toString();
                            }

                            if (url != null && !url.isEmpty()) {
                                datafileField.setValue(url);
                                additionalFieldsList.add(datafileField);
                            }
                        });
                    }


                    additionalFields.setField(additionalFieldsList);
                    entry.setAdditionalFields(additionalFields);
                    entries.addEntry(entry);
                    database.setEntries(entries);

                    omicsDataMarshaller.marshall(database, outputFile);

                } catch (Exception ex) {
                    log.info("Error generating Xml:" + project.getAccession() + "\n" + ex.getMessage());
                }
            }
        }
    }

    private List<Reference> getReferences() {
        List<Reference> references = new ArrayList<>();

        Collection<uk.ac.ebi.pride.archive.dataprovider.reference.Reference> projectReferences = project.getReferencesWithPubmed();
        if (projectReferences != null) {
            for (uk.ac.ebi.pride.archive.dataprovider.reference.Reference reference : projectReferences) {
                Reference xmlReference = new Reference();
                xmlReference.setDbkey(Integer.toString(reference.getPubmedId()));
                xmlReference.setDbname(PUBMED);
                references.add(xmlReference);
            }
        }

        Set<CvParam> specieses = submission.getProjectMetaData().getSpecies();
        if (specieses != null) {
            for (CvParam species : specieses) {
                Reference xmlReference = new Reference();
                xmlReference.setDbkey(species.getAccession());
                xmlReference.setDbname(TAXONOMY);
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
                String orcid = ((Contact) contact).getOrcid();
                if (orcid != null && !orcid.isEmpty()) {
                    Reference xmlReference = new Reference();
                    xmlReference.setDbkey(orcid);
                    xmlReference.setDbname(ORCID);
                    references.add(xmlReference);
                }
            }


        }
        return references;
    }

    private String removeNonPrintableChars(String string){
        return string.replaceAll("\\p{C}", "");
    }
}
