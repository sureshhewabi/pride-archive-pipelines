package uk.ac.ebi.pride.archive.pipeline.tasklets;

import lombok.extern.slf4j.Slf4j;
import org.xml.sax.SAXException;
import uk.ac.ebi.pride.archive.dataprovider.file.ProjectFileSource;
import uk.ac.ebi.pride.archive.dataprovider.project.SubmissionType;
import uk.ac.ebi.pride.archive.pipeline.exceptions.SubmissionUpdateException;
import uk.ac.ebi.pride.archive.px.PostMessage;
import uk.ac.ebi.pride.archive.px.UpdateMessage;
import uk.ac.ebi.pride.archive.px.ValidateMessage;
import uk.ac.ebi.pride.archive.px.xml.XMLParams;
import uk.ac.ebi.pride.archive.repo.models.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.models.project.ProjectSummary;
import uk.ac.ebi.pride.archive.utils.config.FilePathBuilder;
import uk.ac.ebi.pride.archive.utils.streaming.FileUtils;
import uk.ac.ebi.pride.data.exception.SubmissionFileException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Calendar;

/**
 * Updates the PX XML of a project if is public, and make a copy of the original PX XML.
 */
@Slf4j
public class PxXmlUpdater {

    private FileUtils fileUtils;
    private FilePathBuilder filePathBuilder;
    private XMLParams xmlParams;
    private String fileRootLocation;
    private final String pxSchemaVersion="1.4.0";

    /**
     * Constructor that sets the FileUtils, FilePath Builder, XMLParams and file root location.
     * @param fileUtils the new FileUtils to set.
     * @param filePathBuilder  the new FilePathBuilder to set.
     * @param xmlParams the new XMLParams to set.
     * @param fileRootLocation the new file root location to set.
     */
    public PxXmlUpdater(FileUtils fileUtils, FilePathBuilder filePathBuilder, XMLParams xmlParams, String fileRootLocation) {
        this.fileUtils = fileUtils;
        this.filePathBuilder = filePathBuilder;
        this.xmlParams = xmlParams;
        this.fileRootLocation = fileRootLocation;
    }

    public void updatePXXml(ProjectSummary project, File submissionSummaryFile) throws
            SubmissionFileException, IOException, URISyntaxException, SAXException {
        SubmissionType submissionType = SubmissionType.fromString(project.getSubmissionType());
        if (project.isPublicProject() && (submissionType.equals(SubmissionType.COMPLETE) || submissionType.equals(SubmissionType.PARTIAL))) {
            File pxXmlFile = findPXXmlFile(project);
            log.info("Updating PX XML file: " + pxXmlFile.getAbsolutePath());
            String parent = pxXmlFile.getParent();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(project.getPublicationDate());
            int month = calendar.get(Calendar.MONTH) + 1; // the month are zero based, hence the correction +1
            int year = calendar.get(Calendar.YEAR);
            String datasetPathFragment = year + File.separator + (month < 10 ? "0" : "") + month + File.separator + project.getAccession();
            File updatedPxXml = UpdateMessage.updateMetadataPxXml(submissionSummaryFile, new File(parent), project.getAccession(), datasetPathFragment, pxSchemaVersion);
            String validationMessage = ValidateMessage.validateMessage(updatedPxXml, pxSchemaVersion);
            if (validationMessage.equals("")) {  // post updated px xml
                log.info("Updated PX XML validation passed, posting to ProteomeCentral.");
                String response = PostMessage.postFile(updatedPxXml, xmlParams, pxSchemaVersion);
                log.info(response);
                System.out.println(response);
                if (response == null || response.toLowerCase().contains("result=error")
                        || response.startsWith("result=FoundValidationErrors")
                        || response.toLowerCase().contains("internal server error")) {
                    throw new SubmissionUpdateException("Not possible to post XML message " + response);
                }
            } else {
                log.info("Updated PX XML failed to pass validation.");
                throw new SubmissionUpdateException("Updated PX XML failed to pass validation: " + validationMessage);
            }
        } else {
            log.error("PX Project is not public, will not update PX XML");
        }
    }

    /**
     * Finds the PX XML file of a project
     * @param project the project to look up
     * @return a PX XML file
     * @throws FileNotFoundException Problems finding the PX XML file of the project.
     */
    private File findPXXmlFile(ProjectSummary project) throws FileNotFoundException {
        ProjectFile projectFile = new ProjectFile();
        projectFile.setFileName(project.getAccession() + ".xml");
        projectFile.setFileSource(ProjectFileSource.GENERATED);
        return fileUtils.findFileToStream(filePathBuilder.buildPublicationFilePath(fileRootLocation, project, projectFile));
    }
}
