package uk.ac.ebi.pride.archive.pipeline.core.transformers;

import com.mongodb.Mongo;
import org.springframework.beans.factory.annotation.Qualifier;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParamProvider;
import uk.ac.ebi.pride.archive.dataprovider.param.DefaultCvParam;
import uk.ac.ebi.pride.archive.dataprovider.reference.DefaultReference;
import uk.ac.ebi.pride.archive.dataprovider.reference.ReferenceProvider;
import uk.ac.ebi.pride.archive.dataprovider.user.ContactProvider;
import uk.ac.ebi.pride.archive.dataprovider.user.DefaultContact;
import uk.ac.ebi.pride.archive.dataprovider.utils.MSFileTypeConstants;
import uk.ac.ebi.pride.archive.dataprovider.utils.ProjectFolderSourceConstants;
import uk.ac.ebi.pride.archive.dataprovider.utils.TitleConstants;
import uk.ac.ebi.pride.archive.pipeline.utility.StringUtils;
import uk.ac.ebi.pride.archive.repo.repos.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.repos.project.*;
import uk.ac.ebi.pride.data.model.Contact;
import uk.ac.ebi.pride.data.model.CvParam;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.utilities.term.CvTermReference;
import uk.ac.ebi.pride.utilities.util.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
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
public class PrideProjectTransformer {


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
        List<ContactProvider> labHead = oracleProject.getLabHeads()
                .stream()
                .map(contactX -> new DefaultContact(TitleConstants.fromString(contactX.getTitle().getTitle()),
                        contactX.getFirstName(), contactX.getLastName(), contactX.getId().toString(), contactX.getAffiliation(),
                        contactX.getEmail(),  StringUtils.EMPTY_STRING, StringUtils.EMPTY_STRING))
                .collect(Collectors.toList());


        // Get the Submitters data
        List<ContactProvider> submitters = Collections.singletonList(new DefaultContact(TitleConstants.fromString(oracleProject.getSubmitter().getTitle().getTitle()),
                oracleProject.getSubmitter().getFirstName(), oracleProject.getSubmitter().getLastName(), oracleProject.getSubmitter().getId().toString(),
                oracleProject.getSubmitter().getAffiliation(),oracleProject.getSubmitter().getEmail(), StringUtils.EMPTY_STRING, StringUtils.EMPTY_STRING));

        // Get Instruments information
        List<CvParamProvider> instruments =  oracleProject.getInstruments().stream()
                .map(instrumet -> new DefaultCvParam(instrumet.getCvLabel(), instrumet.getAccession(), instrumet.getName(), instrumet.getValue()))
                .collect(Collectors.toList());

        //References
        List<ReferenceProvider> references = oracleProject.getReferences().stream()
                .map( reference -> new DefaultReference(reference.getReferenceLine(), reference.getPubmedId(), reference.getDoi()))
                .collect(Collectors.toList());

        //Modifications
        List<CvParamProvider> ptms = oracleProject.getPtms().stream().map(ptm -> new DefaultCvParam(ptm.getCvLabel(), ptm.getAccession(), ptm.getName(), ptm.getValue()))
                .collect(Collectors.toList());


        //Get software information
        List<CvParamProvider> softwareList = oracleProject.getSoftware()
                .stream()
                .filter(software -> software.getCvParam() != null )
                .map(software -> new DefaultCvParam(software.getCvParam().getCvLabel(), software.getCvParam().getAccession(),
                        software.getCvParam().getName(), software.getCvParam().getValue()))
                .collect(Collectors.toList());

        // Project Tags
        List<String> projectTags = oracleProject.getProjectTags().stream()
                .map(ProjectTag::getTag)
                .map(StringUtils::convertSentenceStyle)
                .collect(Collectors.toList());

        // Project Keywords
        List<String> keywords = Arrays.asList(oracleProject.getKeywords().split(",")).stream()
                .map(StringUtils::convertSentenceStyle).collect(Collectors.toList());

        //Project Quant Methods
        List<CvParamProvider> quantMethods = oracleProject.getQuantificationMethods().stream()
                .filter(quant -> quant.getCvParam() != null)
                .map(quant -> new DefaultCvParam(quant.getCvParam().getCvLabel(), quant.getCvParam().getAccession(),
                        quant.getCvParam().getName(), quant.getCvParam().getValue()))
                .collect(Collectors.toList());

        MongoPrideProject mongoProject = MongoPrideProject.builder()
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
                .build();
        return mongoProject;
    }

    /**
     * Transform a set of Files from Oracle Database into MongoDB
     * @param oracleFiles
     * @param oracleProject
     * @return
     */
    public static List<MongoPrideFile> transformOracleFilesToMongoFiles(List<ProjectFile> oracleFiles,
                                                                        Project oracleProject, String ftpURL,
                                                                        String asperaURL) {
        return oracleFiles.stream().map( oracleFileProject -> transformOracleFileToMongo(oracleFileProject, oracleProject, ftpURL, asperaURL))
                .collect(Collectors.toList());

    }

    /**
     * Transform a file from project in Oracle to a File in MongoDB.
     * @param oracleFileProject The file to be converted
     * @param oracleProject oracle Project
     * @return
     */
    private static MongoPrideFile transformOracleFileToMongo(ProjectFile oracleFileProject, Project oracleProject, String ftpURL, String asperaURL) {
        MSFileTypeConstants fileType = MSFileTypeConstants.OTHER;
        for(MSFileTypeConstants currentFileType: MSFileTypeConstants.values())
            if(currentFileType.getFileType().getName().equalsIgnoreCase(oracleFileProject.getFileType().getName()))
                fileType = currentFileType;
        String folderName = Objects.requireNonNull(ProjectFolderSourceConstants.fromTypeString(oracleFileProject.getFileSource().name())).getFolderName();

        List<CvParamProvider> publicURLs = oracleProject.isPublicProject()?createPublicFileLocations(oracleFileProject.getFileName(), folderName, oracleProject.getPublicationDate(),oracleProject.getAccession(), ftpURL, asperaURL):Collections.EMPTY_LIST;
        return MongoPrideFile.builder()
                .fileName(oracleFileProject.getFileName())
                .fileCategory(fileType.getFileType().getCv())
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
    private static List<CvParamProvider> createPublicFileLocations(String fileName, String fileFolder, Date date, String projectAccession, String ftpURL, String asperaFTP) {
        List<CvParamProvider> cvsPublicURLs = new ArrayList<>();
        if(ftpURL != null && !ftpURL.isEmpty()){
            cvsPublicURLs.add(new DefaultCvParam(CvTermReference.PRIDE_FTP_PROTOCOL_URL.getCvLabel(), CvTermReference.PRIDE_FTP_PROTOCOL_URL.getAccession(), CvTermReference.PRIDE_FTP_PROTOCOL_URL.getName(), buildURL(ftpURL, date, projectAccession, fileName, fileFolder)));
        }
        if(asperaFTP != null && !asperaFTP.isEmpty()){
            cvsPublicURLs.add(new DefaultCvParam(CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getCvLabel(), CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getAccession(), CvTermReference.PRIDE_ASPERA_PROTOCOL_URL.getName(), buildURL(asperaFTP, date, projectAccession, fileName, fileFolder)));
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
        return url.append(protocolURL)
                .append(StringUtils.URL_SEPARATOR)
                .append(year)
                .append(StringUtils.URL_SEPARATOR)
                .append(month)
                .append(StringUtils.URL_SEPARATOR)
                .append(projectAccession)
                .append(StringUtils.URL_SEPARATOR)
                .append(folderName)
                .append(StringUtils.URL_SEPARATOR)
                .append(fileName)
                .toString();

    }

    /**
     * This method transform a project form mongoDB to SolrCloud Project
     * @param mongoPrideProject MongoProject
     * @return SolrCLoud Project
     */
    public static PrideSolrProject transformProjectMongoToSolr(MongoPrideProject mongoPrideProject) {
        PrideSolrProject project = new PrideSolrProject();

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
        project.setLabPIs(new ArrayList<>(mongoPrideProject.getHeadLab()));

        //Get the submitters information
        project.setSubmittersFromNames(new ArrayList<>(mongoPrideProject.getSubmitters()));

        //Get the affiliations
        List<String> affiliations = new ArrayList<>();
        affiliations.addAll(mongoPrideProject.getSubmittersContacts().stream().map(ContactProvider::getAffiliation).collect(Collectors.toList()));
        affiliations.addAll(mongoPrideProject.getLabHeadContacts().stream().map(ContactProvider::getAffiliation).collect(Collectors.toList()));
        project.setAffiliations(affiliations);

        project.setIdentifiedPTMStrings(mongoPrideProject.getPtmList().stream().map(CvParamProvider::getName).collect(Collectors.toSet()));

        /** Set Country **/
        List<String> countries = new ArrayList<>();
        countries.addAll(mongoPrideProject.getLabHeadContacts().stream().map(ContactProvider::getCountry).collect(Collectors.toList()));
        countries.addAll(mongoPrideProject.getSubmittersContacts().stream().map(ContactProvider::getCountry).collect(Collectors.toList()));

        project.setAllCountries(new HashSet<>(countries));

        //Add Dump date
        project.setPublicationDate(mongoPrideProject.getPublicationDate());
        project.setSubmissionDate(mongoPrideProject.getSubmissionDate());
        project.setUpdatedDate(mongoPrideProject.getUpdatedDate());

       //Instruments properties
        project.setInstrumentsFromCvParam(new ArrayList<>(mongoPrideProject.getInstrumentsCvParams()));
        return project;

    }
}
