package uk.ac.ebi.pride.archive.pipeline.core.transformers;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Service;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParamProvider;
import uk.ac.ebi.pride.archive.dataprovider.param.DefaultCvParam;
import uk.ac.ebi.pride.archive.dataprovider.user.ContactProvider;
import uk.ac.ebi.pride.archive.dataprovider.utils.MSFileTypeConstants;
import uk.ac.ebi.pride.archive.dataprovider.utils.ProjectFolderSourceConstants;
import uk.ac.ebi.pride.archive.dataprovider.utils.TitleConstants;
import uk.ac.ebi.pride.archive.pipeline.utility.StringUtils;
import uk.ac.ebi.pride.archive.repo.repos.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.repos.project.*;
import uk.ac.ebi.pride.mongodb.archive.model.PrideArchiveField;
import uk.ac.ebi.pride.mongodb.archive.model.msrun.MongoPrideMSRun;
import uk.ac.ebi.pride.mongodb.archive.model.param.MongoCvParam;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.model.reference.MongoReference;
import uk.ac.ebi.pride.mongodb.archive.model.user.MongoContact;
import uk.ac.ebi.pride.mongodb.utils.PrideMongoUtils;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.utilities.term.CvTermReference;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Autowired
    private static MongoOperations mongo;


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
        List<MongoContact> labHead = oracleProject.getLabHeads()
                .stream()
                .map(contactX -> new MongoContact(TitleConstants.fromString(contactX.getTitle().getTitle()),
                        contactX.getFirstName(), contactX.getLastName(), contactX.getId().toString(), contactX.getAffiliation(),
                        contactX.getEmail(),  StringUtils.EMPTY_STRING, StringUtils.EMPTY_STRING))
                .collect(Collectors.toList());


        // Get the Submitters data
        List<MongoContact> submitters = Collections.singletonList(new MongoContact(TitleConstants.fromString(oracleProject.getSubmitter().getTitle().getTitle()),
                oracleProject.getSubmitter().getFirstName(), oracleProject.getSubmitter().getLastName(), oracleProject.getSubmitter().getId().toString(),
                oracleProject.getSubmitter().getAffiliation(),oracleProject.getSubmitter().getEmail(), StringUtils.EMPTY_STRING, StringUtils.EMPTY_STRING));

        // Get Instruments information
        List<MongoCvParam> instruments =  oracleProject.getInstruments().stream()
                .map(instrumet -> new MongoCvParam(instrumet.getCvLabel(), instrumet.getAccession(), instrumet.getName(), instrumet.getValue()))
                .collect(Collectors.toList());

        //References
        List<MongoReference> references = oracleProject.getReferences().stream()
                .map( reference -> new MongoReference(reference.getReferenceLine(), reference.getPubmedId(), reference.getDoi()))
                .collect(Collectors.toList());

        //Modifications
        List<MongoCvParam> ptms = oracleProject.getPtms().stream().map(ptm -> new MongoCvParam(ptm.getCvLabel(), ptm.getAccession(), ptm.getName(), ptm.getValue()))
                .collect(Collectors.toList());


        //Get software information
        List<MongoCvParam> softwareList = oracleProject.getSoftware()
                .stream()
                .filter(software -> software.getCvParam() != null )
                .map(software -> new MongoCvParam(software.getCvParam().getCvLabel(), software.getCvParam().getAccession(),
                        software.getCvParam().getName(), software.getCvParam().getValue()))
                .collect(Collectors.toList());

        // Project Tags
        List<String> projectTags = oracleProject.getProjectTags().stream()
                .map(ProjectTag::getTag)
                .map(StringUtils::convertSentenceStyle)
                .collect(Collectors.toList());

        // Project Keywords
        List<String> keywords = Arrays.stream(oracleProject.getKeywords().split(","))
                .map(StringUtils::convertSentenceStyle).collect(Collectors.toList());

        //Project Quant Methods
        List<MongoCvParam> quantMethods = oracleProject.getQuantificationMethods().stream()
                .filter(quant -> quant.getCvParam() != null)
                .map(quant -> new MongoCvParam(quant.getCvParam().getCvLabel(), quant.getCvParam().getAccession(),
                        quant.getCvParam().getName(), quant.getCvParam().getValue()))
                .collect(Collectors.toList());

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
                .quantificationMethods(quantMethods)
                .samplesDescription(projectSampleDescription(oracleProject))
                .build();
    }

    /**
     * Transform a set of Files from Oracle Database into MongoDB
     * @param oracleFiles
     * @param oracleProject
     * @return
     */
    public static List<MongoPrideFile> transformOracleFilesToMongoFiles(List<ProjectFile> oracleFiles,
                                                                        List<MongoPrideMSRun> msRunRawFiles,
                                                                        Project oracleProject, String ftpURL,
                                                                        String asperaURL,
                                                                        int accessionSequence) {

        //int finalNumber = PrideMongoUtils.getNextSizedSequence(mongo, PrideArchiveField.PRIDE_FILE_COLLECTION_NAME, oracleFiles.size()) + 1;
        AtomicInteger atominIntegerSequence = new AtomicInteger(accessionSequence);
        return oracleFiles.stream().map( oracleFileProject -> transformOracleFileToMongo(oracleFileProject,msRunRawFiles, oracleProject, ftpURL, asperaURL,atominIntegerSequence.getAndDecrement()))
                .collect(Collectors.toList());

    }

    /**
     * Transform a file from project in Oracle to a File in MongoDB.
     * @param oracleFileProject The file to be converted
     * @param oracleProject oracle Project
     * @return
     */
    private static MongoPrideFile transformOracleFileToMongo(ProjectFile oracleFileProject,List<MongoPrideMSRun> msRunRawFiles, Project oracleProject, String ftpURL, String asperaURL, int finalNumber) {
        MSFileTypeConstants fileType = MSFileTypeConstants.OTHER;
        for(MSFileTypeConstants currentFileType: MSFileTypeConstants.values())
            if(currentFileType.getFileType().getName().equalsIgnoreCase(oracleFileProject.getFileType().getName()))
                fileType = currentFileType;
        String folderName = Objects.requireNonNull(ProjectFolderSourceConstants.fromTypeString(oracleFileProject.getFileSource().name())).getFolderName();
        NumberFormat formatter = new DecimalFormat("00000000000");

        //Accession should be common if a same file goes into file and msrun collection
        String accession = "PXF" + formatter.format(finalNumber);

        List<MongoCvParam> publicURLs = oracleProject.isPublicProject()?createPublicFileLocations(oracleFileProject.getFileName(),
                folderName, oracleProject.getPublicationDate(),oracleProject.getAccession(), ftpURL, asperaURL):Collections.emptyList();

        //check for MSRun files as they need to be stored in file collection and ms run collection
        if(fileType.getFileType().getName().equals(MSFileTypeConstants.RAW.getFileType().getName())){
            msRunRawFiles.add(MongoPrideMSRun.builder()
                    .accession(accession)
                    .fileName(oracleFileProject.getFileName())
                    .projectAccessions(Collections.singleton(oracleProject.getAccession()))
                    .build());
        }

        return MongoPrideFile.builder()
                .accession(accession)
                .fileName(oracleFileProject.getFileName())
                .fileCategory(new MongoCvParam(fileType.getFileType().getCv().getCvLabel(), fileType.getFileType().getCv().getAccession(),
                        fileType.getFileType().getCv().getName(), fileType.getFileType().getCv().getValue()))
                .fileSourceFolder(oracleFileProject.getFileSource().name())
                .projectAccessions(Collections.singleton(oracleProject.getAccession()))
                .fileSizeBytes(oracleFileProject.getFileSize())
                .publicationDate(oracleProject.getPublicationDate())
                .fileSourceType(oracleFileProject.getFileSource().name())
                .fileSourceFolder(folderName)
                .publicFileLocations(publicURLs)
                .submissionDate(oracleProject.getSubmissionDate())
                .updatedDate(oracleProject.getUpdateDate())
                .build();
    }

    /**
     * In oracle the public URLs are build on the fly by the web service or other services. In mongo, the Public URLs contains the
     * information of the public files.
     * @param fileName file Name
     * @param fileFolder file Folder (generated, submitted)
     * @param date Publication Date
     * @param projectAccession Project Accession
     * @param ftpURL ftp prefix
     * @param asperaFTP aspera prefix
     * @return
     */
    private static List<MongoCvParam> createPublicFileLocations(String fileName, String fileFolder, Date date, String projectAccession, String ftpURL, String asperaFTP) {
        List<MongoCvParam> cvsPublicURLs = new ArrayList<>();
        if(ftpURL != null && !ftpURL.isEmpty()){
            cvsPublicURLs.add(new MongoCvParam(CvTermReference.PRIDE_FTP_PROTOCOL_URL.getCvLabel(), CvTermReference.PRIDE_FTP_PROTOCOL_URL.getAccession(), CvTermReference.PRIDE_FTP_PROTOCOL_URL.getName(), buildURL(ftpURL, date, projectAccession, fileName, fileFolder)));
        }
        if(asperaFTP != null && !asperaFTP.isEmpty()){
            cvsPublicURLs.add(new MongoCvParam(CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getCvLabel(), CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getAccession(), CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getName(), buildURL(asperaFTP, date, projectAccession, fileName, fileFolder)));
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
        if(!folderName.equalsIgnoreCase(ProjectFolderSourceConstants.SUBMITTED.getFolderName())){
                    url.append(folderName)
                    .append(StringUtils.URL_SEPARATOR);
        }
        url.append(fileName);
        return url.toString();
    }

    /**
     * This method transform a project form mongoDB to SolrCloud Project
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

        /** Set PTMs **/
        project.setIdentifiedPTMStringsFromCvParam(mongoPrideProject.getPtmList()
                .stream()
                .map(x -> new DefaultCvParam(x.getCvLabel(), x.getAccession(), x.getName(), x.getValue()))
                .collect(Collectors.toList())
        );

        /** Set Country **/
        Set<String> countries = new HashSet<>();
        countries.addAll(mongoPrideProject.getLabHeadContacts().stream().map(ContactProvider::getCountry).collect(Collectors.toList()));
        countries.addAll(mongoPrideProject.getSubmittersContacts().stream().map(ContactProvider::getCountry).collect(Collectors.toList()));

        if(mongoPrideProject.getCountries() != null)
            countries.addAll(mongoPrideProject.getCountries());

        project.setAllCountries(countries.stream().filter(x-> !x.isEmpty()).collect(Collectors.toSet()));

        //Add Dump date
        project.setPublicationDate(mongoPrideProject.getPublicationDate());
        project.setSubmissionDate(mongoPrideProject.getSubmissionDate());
        project.setUpdatedDate(mongoPrideProject.getUpdatedDate());

       //Instruments properties
        project.setInstrumentsFromCvParam(new ArrayList<>(mongoPrideProject.getInstrumentsCvParams()));
        List<Tuple<CvParamProvider, List<CvParamProvider>>> sampleAttributes = new ArrayList<>();
        mongoPrideProject.getSamplesDescription()
                .forEach(x ->
                        sampleAttributes.add(new Tuple( new DefaultCvParam(x.getKey().getCvLabel(),
                        x.getKey().getAccession(),
                        x.getKey().getName(),
                        x.getKey().getValue()),
                        x.getValue()
                                .stream()
                                .map(value -> new DefaultCvParam(value.getCvLabel(), value.getAccession(), value.getName(), value.getValue()))
                                .collect(Collectors.toList())))
        );
        project.setSampleAttributes(sampleAttributes);

        return project;
    }

    /**
     * Mapping from old PRIDE Sample processing to a new Data model.
     * @param oracleProject Oracle Project
     * @return Mapping of new Sample Data
     */
    public static List<Tuple<MongoCvParam, List<MongoCvParam>>> projectSampleDescription(Project oracleProject){
        Map<MongoCvParam, List<MongoCvParam>> projectSampleProcessing = new HashMap<>();
        oracleProject.getSamples().forEach(projectSampleCvParam -> {
            if(projectSampleCvParam != null){
                MongoCvParam key = null;
                MongoCvParam value = new MongoCvParam(projectSampleCvParam.getCvLabel(),
                        projectSampleCvParam.getAccession(), projectSampleCvParam.getName(),
                        projectSampleCvParam.getValue());
                if(projectSampleCvParam.getCvLabel().equalsIgnoreCase(StringUtils.CV_LABEL_ORGANISM)){
                    key = new MongoCvParam(CvTermReference.EFO_ORGANISM.getCvLabel(),
                           CvTermReference.EFO_ORGANISM.getAccession(),
                           CvTermReference.EFO_ORGANISM.getName(), null);
                }else if(projectSampleCvParam.getCvLabel().equalsIgnoreCase(StringUtils.CV_LABEL_CELL_COMPONENT) ||
                        projectSampleCvParam.getCvLabel().equalsIgnoreCase(StringUtils.CV_LABEL_CELL_TISSUE)){
                    key = new MongoCvParam(CvTermReference.EFO_ORGANISM_PART.getCvLabel(),
                            CvTermReference.EFO_ORGANISM_PART.getAccession(),
                            CvTermReference.EFO_ORGANISM_PART.getName(), null);
                }else if(projectSampleCvParam.getCvLabel().equalsIgnoreCase(StringUtils.CV_LABEL_DISEASE)){
                    key = new MongoCvParam(CvTermReference.EFO_DISEASE.getCvLabel(),
                            CvTermReference.EFO_DISEASE.getAccession(),
                            CvTermReference.EFO_DISEASE.getName(), null);
                }

                if(key != null){
                    List<MongoCvParam> sampleValues = projectSampleProcessing.get(key);
                    if(sampleValues == null)
                        sampleValues = new ArrayList<>();
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
}
