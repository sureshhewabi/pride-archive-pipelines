package uk.ac.ebi.pride.archive.pipeline.core.transformers;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Service;
import uk.ac.ebi.pride.archive.dataprovider.assay.AssayType;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.dataprovider.file.ProjectFileSource;
import uk.ac.ebi.pride.archive.dataprovider.file.ProjectFileType;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParam;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParamProvider;
import uk.ac.ebi.pride.archive.dataprovider.reference.Reference;
import uk.ac.ebi.pride.archive.dataprovider.user.Contact;
import uk.ac.ebi.pride.archive.dataprovider.user.ContactProvider;
import uk.ac.ebi.pride.archive.dataprovider.utils.MSFileTypeConstants;
import uk.ac.ebi.pride.archive.dataprovider.utils.ProjectFolderSourceConstants;
import uk.ac.ebi.pride.archive.dataprovider.utils.TitleConstants;
import uk.ac.ebi.pride.archive.pipeline.utility.CalculateSha1ChecksumForFile;
import uk.ac.ebi.pride.archive.pipeline.utility.StringUtils;
import uk.ac.ebi.pride.archive.repo.repos.assay.Assay;
import uk.ac.ebi.pride.archive.repo.repos.assay.AssayPTM;
import uk.ac.ebi.pride.archive.repo.repos.assay.software.Software;
import uk.ac.ebi.pride.archive.repo.repos.assay.software.SoftwareCvParam;
import uk.ac.ebi.pride.archive.repo.repos.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.repos.project.Project;
import uk.ac.ebi.pride.archive.repo.repos.project.ProjectTag;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoAssayFile;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoPrideAssay;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.msrun.MongoPrideMSRun;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.utilities.term.CvTermReference;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 08/06/2018.
 */

@Service
public class PrideProjectTransformer {

    public static final String SUBMITTED = "submitted";
    public static final String GENERATED = "generated";
    public static final String PUBLICATION_DATE = "publicationDate";
    public static final String PROJECT_ACCESSION = "projectAccession";
    public static final String FILE_NAME = "fileName";
    @Autowired
    private static MongoOperations mongo;

    private static final String submittedFilePath = "/nfs/pride/prod/archive/publicationDate/projectAccession/submitted/fileName";


    /**
     * An oracle project in pride will be converted into a MongoDB representation. Some of the information in the Oracle Project aare represented in a different way in
     * MongoDB.
     *
     * @param oracleProject Oracle PRIDE Project
     * @return MongoPrideProject
     */
    public static MongoPrideProject transformOracleToMongo(Project oracleProject) {

        // Before creating the project some complex data structures should be created

        // Get the Lab Heads
        List<Contact> labHead = oracleProject.getLabHeads()
                .stream()
                .map(contactX -> new Contact(TitleConstants.fromString(contactX.getTitle().getTitle()),
                        contactX.getFirstName(), contactX.getLastName(), contactX.getId().toString(), contactX.getAffiliation(),
                        contactX.getEmail(), StringUtils.EMPTY_STRING, StringUtils.EMPTY_STRING))
                .collect(Collectors.toList());


        // Get the Submitters data
        List<Contact> submitters = Collections.singletonList(new Contact(TitleConstants.fromString(oracleProject.getSubmitter().getTitle().getTitle()),
                oracleProject.getSubmitter().getFirstName(), oracleProject.getSubmitter().getLastName(), oracleProject.getSubmitter().getId().toString(),
                oracleProject.getSubmitter().getAffiliation(), oracleProject.getSubmitter().getEmail(), StringUtils.EMPTY_STRING, StringUtils.EMPTY_STRING));

        // Get Instruments information
        Set<CvParam> instruments = oracleProject.getInstruments().stream()
                .map(instrumet -> new CvParam(instrumet.getCvLabel(), instrumet.getAccession(), instrumet.getName(), instrumet.getValue()))
                .collect(Collectors.toSet());

        //References
        List<Reference> references = oracleProject.getReferences().stream()
                .map(reference -> new Reference(reference.getReferenceLine(), reference.getPubmedId(), reference.getDoi()))
                .collect(Collectors.toList());

        //Modifications
        Set<CvParam> ptms = oracleProject.getPtms().stream()
                .map(ptm -> new CvParam(ptm.getCvLabel(), ptm.getAccession(), ptm.getName(), ptm.getValue()))
                .collect(Collectors.toSet());


        //Get software information
        Set<CvParam> softwareList = oracleProject.getSoftware()
                .stream()
                .filter(software -> software.getCvParam() != null)
                .map(software -> new CvParam(software.getCvParam().getCvLabel(), software.getCvParam().getAccession(),
                        software.getCvParam().getName(), software.getCvParam().getValue()))
                .collect(Collectors.toSet());

        // Project Tags
        List<String> projectTags = oracleProject.getProjectTags().stream()
                .map(ProjectTag::getTag)
                .map(StringUtils::convertSentenceStyle)
                .collect(Collectors.toList());

        // Project Keywords
        List<String> keywords = oracleProject.getKeywords().stream()
                .map(StringUtils::convertSentenceStyle).collect(Collectors.toList());

        //Project Quant Methods
        Set<CvParam> quantMethods = oracleProject.getQuantificationMethods().stream()
                .filter(quant -> quant.getCvParam() != null)
                .map(quant -> new CvParam(quant.getCvParam().getCvLabel(), quant.getCvParam().getAccession(),
                        quant.getCvParam().getName(), quant.getCvParam().getValue()))
                .collect(Collectors.toSet());


        return MongoPrideProject.builder()
                .title(oracleProject.getTitle())
                .accession(oracleProject.getAccession())
                .description(oracleProject.getProjectDescription())
                .sampleProcessing(oracleProject.getSampleProcessingProtocol())
                .dataProcessing(oracleProject.getDataProcessingProtocol())
                .keywords(keywords)
                .submissionDate(oracleProject.getSubmissionDate())
                .updatedDate(oracleProject.getUpdateDate())
                .publicationDate(oracleProject.getPublicationDate())
                .headLab(labHead)
                .submitters(submitters)
                .instruments(instruments)
                .references(references)
                .ptmList(ptms)
                .softwareList(softwareList)
                .projectTags(projectTags)
                .submissionType(oracleProject.getSubmissionType())
                .quantificationMethods(quantMethods)
                .samplesDescription(projectSampleDescription(oracleProject))
                .publicProject(oracleProject.isPublicProject())
                .build();
    }

    /**
     * Transform a set of Files from Oracle Database into MongoDB
     *
     * @param oracleFiles
     * @param oracleProject
     * @return
     */
    public static List<MongoPrideFile> transformOracleFilesToMongoFiles(List<ProjectFile> oracleFiles,
                                                                        List<MongoPrideMSRun> msRunRawFiles,
                                                                        Project oracleProject, String ftpURL,
                                                                        String asperaURL) {
        return oracleFiles.stream().map(oracleFileProject -> transformOracleFileToMongo(oracleFileProject, msRunRawFiles, oracleProject, ftpURL, asperaURL))
                .collect(Collectors.toList());

    }

    /**
     * Transform a file from project in Oracle to a File in MongoDB.
     *
     * @param oracleFileProject The file to be converted
     * @param oracleProject     oracle Project
     * @return
     */
    private static MongoPrideFile transformOracleFileToMongo(ProjectFile oracleFileProject, List<MongoPrideMSRun> msRunRawFiles, Project oracleProject, String ftpURL, String asperaURL) {
        MSFileTypeConstants fileType = MSFileTypeConstants.OTHER;
        for (MSFileTypeConstants currentFileType : MSFileTypeConstants.values())
            if (currentFileType.getFileType().getName().equalsIgnoreCase(oracleFileProject.getFileType().getName()))
                fileType = currentFileType;
        String folderName = Objects.requireNonNull(ProjectFolderSourceConstants.fromTypeString(oracleFileProject.getFileSource().name())).getFolderName();
        NumberFormat formatter = new DecimalFormat("00000000000");

        Set<CvParam> publicURLs = oracleProject.isPublicProject() ? createPublicFileLocations(oracleFileProject.getFileName(),
                folderName, oracleProject.getPublicationDate(), oracleProject.getAccession(), ftpURL, asperaURL) : Collections.emptySet();

        String md5checkSum = getSha1Checksum(oracleFileProject, oracleProject, folderName);
        String accession = oracleProject.getAccession() + "_" +  md5checkSum;

        //check for MSRun files as they need to be stored in file collection and ms run collection
        if (fileType.getFileType().getName().equals(MSFileTypeConstants.RAW.getFileType().getName())) {
            msRunRawFiles.add(MongoPrideMSRun.builder()
                    .accession(accession)
                    .fileName(oracleFileProject.getFileName())
                    .fileSizeBytes(oracleFileProject.getFileSize())
                    .projectAccessions(Collections.singleton(oracleProject.getAccession()))
                    .build());
        }


        return MongoPrideFile.builder()
                .accession(accession)
                .fileName(oracleFileProject.getFileName())
                .fileCategory(new CvParam(fileType.getFileType().getCv().getCvLabel(), fileType.getFileType().getCv().getAccession(),
                        fileType.getFileType().getCv().getName(), fileType.getFileType().getCv().getValue()))
                .fileSourceFolder(oracleFileProject.getFileSource().name())
                .projectAccessions(Collections.singleton(oracleProject.getAccession()))
                .fileSizeBytes(oracleFileProject.getFileSize())
                .publicationDate(oracleProject.getPublicationDate())
                .fileSourceType(oracleFileProject.getFileSource().name())
                .fileSourceFolder(folderName)
                .md5Checksum(md5checkSum)
                .publicFileLocations(publicURLs)
                .submissionDate(oracleProject.getSubmissionDate())
                .updatedDate(oracleProject.getUpdateDate())
                .build();
    }

    private static String getSha1Checksum(ProjectFile oracleFileProject, Project oracleProject, String folderName) {
        String sha1Checksum = null;
        String filePath = submittedFilePath;
        if (!folderName.equals(SUBMITTED)) {
            filePath = filePath.replace(SUBMITTED, GENERATED);
        }
        if (oracleProject.isPublicProject()) {
            Calendar calendar = new GregorianCalendar();
            calendar.setTime(oracleProject.getPublicationDate());
            filePath = filePath.replace(PUBLICATION_DATE, calendar.get(Calendar.YEAR) + "/" + String.format("%02d", calendar.get(Calendar.MONTH)));
        } else {
            filePath = filePath.replace(PUBLICATION_DATE + "\\/", "");
        }
        filePath = filePath.replace(PROJECT_ACCESSION, oracleProject.getAccession());
        filePath = filePath.replace(FILE_NAME, oracleFileProject.getFileName());
        try {
            sha1Checksum = CalculateSha1ChecksumForFile.calculateSha1Checksum(filePath);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sha1Checksum;
    }

    /**
     * In oracle the public URLs are build on the fly by the web service or other services. In mongo, the Public URLs contains the
     * information of the public files.
     *
     * @param fileName         file Name
     * @param fileFolder       file Folder (generated, submitted)
     * @param date             Publication Date
     * @param projectAccession Project Accession
     * @param ftpURL           ftp prefix
     * @param asperaFTP        aspera prefix
     * @return
     */
    private static Set<CvParam> createPublicFileLocations(String fileName, String fileFolder, Date date, String projectAccession, String ftpURL, String asperaFTP) {
        Set<CvParam> cvsPublicURLs = new HashSet<>();
        if (ftpURL != null && !ftpURL.isEmpty()) {
            cvsPublicURLs.add(new CvParam(CvTermReference.PRIDE_FTP_PROTOCOL_URL.getCvLabel(), CvTermReference.PRIDE_FTP_PROTOCOL_URL.getAccession(), CvTermReference.PRIDE_FTP_PROTOCOL_URL.getName(), buildURL(ftpURL, date, projectAccession, fileName, fileFolder)));
        }
        if (asperaFTP != null && !asperaFTP.isEmpty()) {
            cvsPublicURLs.add(new CvParam(CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getCvLabel(), CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getAccession(), CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getName(), buildURL(asperaFTP, date, projectAccession, fileName, fileFolder)));
        }
        return cvsPublicURLs;
    }

    /**
     * Build the path for a Project based on the protocol (ftp, aspera or nfs), a publication date, a project accession, a file Name and the folderName.
     * Todo: This function can be probably move to another utilities package
     *
     * @param protocolURL
     * @param publicationDate
     * @param fileName
     * @param folderName
     * @return
     */
    public static String buildURL(String protocolURL, Date publicationDate, String projectAccession, String fileName, String folderName) {
        SimpleDateFormat simpleDateformat = new SimpleDateFormat("MM");
        String month = simpleDateformat.format(publicationDate);
        simpleDateformat = new SimpleDateFormat("yyyy");
        String year = simpleDateformat.format(publicationDate);
        StringBuilder url = new StringBuilder();
        url.append(protocolURL)
                .append(StringUtils.URL_SEPARATOR)
                .append(year)
                .append(StringUtils.URL_SEPARATOR)
                .append(month)
                .append(StringUtils.URL_SEPARATOR)
                .append(projectAccession)
                .append(StringUtils.URL_SEPARATOR);
        if (!folderName.equalsIgnoreCase(ProjectFolderSourceConstants.SUBMITTED.getFolderName())) {
            url.append(folderName)
                    .append(StringUtils.URL_SEPARATOR);
        }
        url.append(fileName);
        return url.toString();
    }

    /**
     * This method transform a project form mongoDB to SolrCloud Project
     *
     * @param mongoPrideProject MongoProject
     * @return SolrCLoud Project
     */
    public static PrideSolrProject transformProjectMongoToSolr(MongoPrideProject mongoPrideProject) {
        PrideSolrProject project = new PrideSolrProject();

        project.setId(mongoPrideProject.getId().toString());

        //Get accession, title, keywords, Data and Sample protocols
        project.setAccession(mongoPrideProject.getAccession());
        project.setTitle(mongoPrideProject.getTitle());
        project.setKeywords(new ArrayList<>(mongoPrideProject.getKeywords()));
        project.setDataProcessingProtocol(mongoPrideProject.getDataProcessingProtocol());
        project.setSampleProcessingProtocol(mongoPrideProject.getSampleProcessingProtocol());
        project.setProjectDescription(mongoPrideProject.getDescription());

        //Get project Tags
        project.setProjectTags(new ArrayList<>(mongoPrideProject.getProjectTags()));

        //Get the researchers
        project.setLabPIs(new HashSet<>(mongoPrideProject.getHeadLab()));

        //Get the submitters information
        project.setSubmittersFromNames(new ArrayList<>(mongoPrideProject.getSubmitters()));

        //Get the affiliations
        Set<String> affiliations = new HashSet<>();
        affiliations.addAll(mongoPrideProject.getSubmittersContacts().stream().map(ContactProvider::getAffiliation).collect(Collectors.toList()));
        affiliations.addAll(mongoPrideProject.getLabHeadContacts().stream().map(ContactProvider::getAffiliation).collect(Collectors.toList()));
        project.setAffiliations(affiliations);

        // Set PTMs
        project.setIdentifiedPTMStringsFromCvParam(mongoPrideProject.getPtmList()
                .stream()
                .map(x -> new CvParam(x.getCvLabel(), x.getAccession(), x.getName(), x.getValue()))
                .collect(Collectors.toList())
        );

        // Set Country
        Set<String> countries = new HashSet<>();
        countries.addAll(mongoPrideProject.getLabHeadContacts().stream().map(ContactProvider::getCountry).collect(Collectors.toList()));
        countries.addAll(mongoPrideProject.getSubmittersContacts().stream().map(ContactProvider::getCountry).collect(Collectors.toList()));

        if (mongoPrideProject.getCountries() != null)
            countries.addAll(mongoPrideProject.getCountries());

        project.setAllCountries(countries.stream().filter(x -> !x.isEmpty()).collect(Collectors.toSet()));

        //Add Dump date
        project.setPublicationDate(mongoPrideProject.getPublicationDate());
        project.setSubmissionDate(mongoPrideProject.getSubmissionDate());
        project.setSubmissionType(mongoPrideProject.getSubmissionType());
        project.setUpdatedDate(mongoPrideProject.getUpdatedDate());

        //Instruments properties
        project.setInstrumentsFromCvParam(new ArrayList<>(mongoPrideProject.getInstrumentsCvParams()));
        List<Tuple<CvParamProvider, List<CvParamProvider>>> sampleAttributes = new ArrayList<>();
        mongoPrideProject.getSamplesDescription()
                .forEach(x ->
                        sampleAttributes.add(new Tuple(new CvParam(x.getKey().getCvLabel(),
                                x.getKey().getAccession(),
                                x.getKey().getName(),
                                x.getKey().getValue()),
                                x.getValue()
                                        .stream()
                                        .map(value -> new CvParam(value.getCvLabel(), value.getAccession(), value.getName(), value.getValue()))
                                        .collect(Collectors.toList())))
                );
        project.setSampleAttributes(sampleAttributes);
        project.setReferences(new HashSet<>(mongoPrideProject.getReferences()));

        return project;
    }

    /**
     * Mapping from old PRIDE Sample processing to a new Data model.
     *
     * @param oracleProject Oracle Project
     * @return Mapping of new Sample Data
     */
    public static List<Tuple<CvParam, Set<CvParam>>> projectSampleDescription(Project oracleProject) {
        Map<CvParam, Set<CvParam>> projectSampleProcessing = new HashMap<>();
        oracleProject.getSamples().forEach(projectSampleCvParam -> {
            if (projectSampleCvParam != null) {
                CvParam key = null;
                CvParam value = new CvParam(projectSampleCvParam.getCvLabel(),
                        projectSampleCvParam.getAccession(), projectSampleCvParam.getName(),
                        projectSampleCvParam.getValue());
                if (projectSampleCvParam.getCvLabel().equalsIgnoreCase(StringUtils.CV_LABEL_ORGANISM)) {
                    key = new CvParam(CvTermReference.EFO_ORGANISM.getCvLabel(),
                            CvTermReference.EFO_ORGANISM.getAccession(),
                            CvTermReference.EFO_ORGANISM.getName(), null);
                } else if (projectSampleCvParam.getCvLabel().equalsIgnoreCase(StringUtils.CV_LABEL_CELL_COMPONENT) ||
                        projectSampleCvParam.getCvLabel().equalsIgnoreCase(StringUtils.CV_LABEL_CELL_TISSUE)) {
                    key = new CvParam(CvTermReference.EFO_ORGANISM_PART.getCvLabel(),
                            CvTermReference.EFO_ORGANISM_PART.getAccession(),
                            CvTermReference.EFO_ORGANISM_PART.getName(), null);
                } else if (projectSampleCvParam.getCvLabel().equalsIgnoreCase(StringUtils.CV_LABEL_DISEASE)) {
                    key = new CvParam(CvTermReference.EFO_DISEASE.getCvLabel(),
                            CvTermReference.EFO_DISEASE.getAccession(),
                            CvTermReference.EFO_DISEASE.getName(), null);
                }

                if (key != null) {
                    Set<CvParam> sampleValues = projectSampleProcessing.get(key);
                    if (sampleValues == null)
                        sampleValues = new HashSet<>();
                    sampleValues.add(value);
                    projectSampleProcessing.put(key, sampleValues);
                }
            }
        });

        return projectSampleProcessing
                .entrySet()
                .stream()
                .map(x -> new Tuple<>(x.getKey(), x.getValue()))
                .collect(Collectors.toList());
    }

    public static List<MongoPrideAssay> transformOracleAssayToMongo(List<Assay> assays, List<ProjectFile> files, List<MongoPrideFile> mongoFiles, Project project) {
        List<MongoPrideAssay> mongoAssays = new ArrayList<>();
        for (Assay assay : assays) {
            // Get the software information
            Set<CvParam> softwareMongo = new HashSet<>();
            if (assay.getSoftwares() != null && assay.getSoftwares().size() > 0) {
                for (Software software : assay.getSoftwares()) {
                    if (software.getSoftwareCvParams() != null && software.getSoftwareCvParams().size() > 0) {
                        for (SoftwareCvParam cvParam : software.getSoftwareCvParams()) {
                            if (cvParam != null && cvParam.getCvParam() != null) {
                                boolean presentTool = softwareMongo
                                        .stream()
                                        .anyMatch(x -> x.getAccession().equalsIgnoreCase(cvParam.getCvParam().getAccession()));
                                if (!presentTool) {
                                    CvParam mongoTool = new CvParam(cvParam.getCvParam().getCvLabel(),
                                            cvParam.getCvParam().getAccession(),
                                            cvParam.getCvParam().getName(), cvParam.getCvParam().getValue());
                                    softwareMongo.add(mongoTool);
                                }
                            }
                        }
                    }
                }
            }

            //Get PTMs information
            List<Tuple<CvParam, Integer>> ptms = new ArrayList<>();
            if (assay.getPtms() != null && assay.getPtms().size() > 0) {
                for (AssayPTM ptm : assay.getPtms()) {
                    if (ptm.getCvParam() != null) {
                        CvParam ptmMongo = new CvParam(ptm.getCvParam().getCvLabel(),
                                ptm.getCvParam().getAccession(),
                                ptm.getCvParam().getName(), ptm.getCvParam().getValue());
                        ptms.add(new Tuple<>(ptmMongo, 0));

                    }

                }

            }

            //Initiation of Summary parameters
            Set<CvParam> summaryResults = new HashSet<>();
            summaryResults.add(new CvParam(CvTermReference.PRIDE_NUMBER_ID_PROTEINS.getCvLabel(),
                    CvTermReference.PRIDE_NUMBER_ID_PROTEINS.getAccession(),
                    CvTermReference.PRIDE_NUMBER_ID_PROTEINS.getName(),
                    String.valueOf(assay.getProteinCount())));

            summaryResults.add(new CvParam(CvTermReference.PRIDE_NUMBER_ID_PEPTIDES.getCvLabel(),
                    CvTermReference.PRIDE_NUMBER_ID_PEPTIDES.getAccession(),
                    CvTermReference.PRIDE_NUMBER_ID_PEPTIDES.getName(),
                    String.valueOf(assay.getPeptideCount())));

            summaryResults.add(new CvParam(CvTermReference.PRIDE_NUMBER_ID_PSMS.getCvLabel(),
                    CvTermReference.PRIDE_NUMBER_ID_PSMS.getAccession(),
                    CvTermReference.PRIDE_NUMBER_ID_PSMS.getName(),
                    String.valueOf(0)));

            summaryResults.add(new CvParam(CvTermReference.PRIDE_NUMBER_MODIFIED_PEPTIDES.getCvLabel(),
                    CvTermReference.PRIDE_NUMBER_MODIFIED_PEPTIDES.getAccession(),
                    CvTermReference.PRIDE_NUMBER_MODIFIED_PEPTIDES.getName(),
                    String.valueOf(0)));

            List<MongoAssayFile> prideFiles = getMongoAssayFile(assay.getId(), files, mongoFiles);

            MongoPrideAssay mongoPrideAssay = MongoPrideAssay.builder()
                    .assayType(AssayType.IDENTIFICATION)
                    .accession(assay.getAccession())
                    .title(project.getTitle())
                    .projectAccessions(Collections.singleton(project.getAccession()))
                    .analysisAccessions(Collections.singleton(project.getAccession()))
                    .dataAnalysisSoftwares(softwareMongo)
                    .summaryResults(summaryResults)
                    .ptmsResults(ptms)
                    .assayFiles(prideFiles)
                    .build();
            mongoAssays.add(mongoPrideAssay);
        }
        return mongoAssays;
    }

    private static List<MongoAssayFile> getMongoAssayFile(Long id, List<ProjectFile> files, List<MongoPrideFile> mongoFiles) {
        List<MongoAssayFile> mongoAssayFiles = new ArrayList<>();

        List<Tuple<ProjectFile, MongoPrideFile>> filterFiles = files.stream()
                .filter(x -> x.getAssayId() != null && x.getAssayId().longValue() == id.longValue()).map(x -> {
                    Optional<MongoPrideFile> mongoPrideFile = mongoFiles.stream()
                            .filter(y -> y.getFileName().equalsIgnoreCase(x.getFileName())).findFirst();
                    return mongoPrideFile.map(mongoPrideFile1 -> new Tuple<>(x, mongoPrideFile1)).orElse(null);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        for (int i = 0; i < filterFiles.size(); i++) {
            Tuple<ProjectFile, MongoPrideFile> oracleFile = filterFiles.get(i);
            if ((oracleFile.getKey().getFileSource() == ProjectFileSource.GENERATED &&
                    oracleFile.getKey().getFileName().contains("pride.mztab")) ||
                    (oracleFile.getKey().getFileType() == ProjectFileType.RESULT)) {

                MongoAssayFile resultFile = MongoAssayFile.builder()
                        .fileName(oracleFile.getKey().getFileName())
                        .fileAccession(oracleFile.getValue().getAccession())
                        .fileCategory((CvParam) oracleFile.getValue().getFileCategory())
                        .build();
                List<MongoAssayFile> relatedFiles = new ArrayList<>();
                for (int j = 0; j < filterFiles.size(); j++) {
                    Tuple<ProjectFile, MongoPrideFile> peakFile = filterFiles.get(j);
                    if (oracleFile.getKey().getFileSource() == ProjectFileSource.GENERATED &&
                            oracleFile.getKey().getFileName().contains("pride.mztab") &&
                            peakFile.getKey().getFileSource() == ProjectFileSource.GENERATED && !peakFile.getKey().getFileName().contains("pride.mztab")) {

                        MongoAssayFile mgfFile = MongoAssayFile.builder()
                                .fileAccession(peakFile.getValue().getAccession())
                                .fileCategory((CvParam) peakFile.getValue().getFileCategory())
                                .fileName(peakFile.getKey().getFileName())
                                .build();
                        relatedFiles.add(mgfFile);
                    }
                    if (oracleFile.getKey().getFileType() == ProjectFileType.RESULT &&
                            peakFile.getKey().getFileSource() == ProjectFileSource.SUBMITTED && peakFile.getKey().getFileType() == ProjectFileType.PEAK) {

                        MongoAssayFile mgfFile = MongoAssayFile.builder()
                                .fileAccession(peakFile.getValue().getAccession())
                                .fileCategory((CvParam) peakFile.getValue().getFileCategory())
                                .fileName(peakFile.getKey().getFileName())
                                .build();
                        relatedFiles.add(mgfFile);

                    }
                }
                resultFile.setRelatedFiles(relatedFiles);
                mongoAssayFiles.add(resultFile);
            }
        }

        return mongoAssayFiles;
    }
}
