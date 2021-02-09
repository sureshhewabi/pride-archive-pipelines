package uk.ac.ebi.pride.archive.pipeline.tasklets;

import lombok.extern.slf4j.Slf4j;
import uk.ac.ebi.pride.archive.dataprovider.project.SubmissionType;
import uk.ac.ebi.pride.archive.repo.models.project.ProjectSummary;
import uk.ac.ebi.pride.data.exception.SubmissionFileException;
import uk.ac.ebi.pride.data.io.SubmissionFileParser;
import uk.ac.ebi.pride.data.io.SubmissionFileWriter;
import uk.ac.ebi.pride.data.model.Submission;
import uk.ac.ebi.pride.data.validation.SubmissionValidator;
import uk.ac.ebi.pride.data.validation.ValidationMessage;
import uk.ac.ebi.pride.data.validation.ValidationReport;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

/**
 * @author Suresh Hewapathirana
 */
@Slf4j
public class SubmissionSummaryFileUpdater {


    private final Calendar PRIDE_3_RELEASE_DATE = Calendar.getInstance();

    private SubmissionUpdater submissionUpdater;

    /**
     * Constructor that sets the SubmissionUpdater.
     * @param submissionUpdater the SubmissionUpdater to set.
     */
    public SubmissionSummaryFileUpdater(SubmissionUpdater submissionUpdater) {
        PRIDE_3_RELEASE_DATE.set(2014, Calendar.JANUARY, 6);
        this.submissionUpdater = submissionUpdater;
    }

    /**
     * Performs the update of a submission.px file from a ProjectSummary object. The "old" submission.px file will
     * be backed up.
     * @param project the project with the new information to use for updating.
     * @param submissionSummaryFile the submission.px file that is to be updated.
     * @throws SubmissionFileException Unable to parse the submission.px file to be read as Submission object.
     */
    public void updateSubmissionSummaryFile(ProjectSummary project, File submissionSummaryFile) throws Exception {
        Submission submissionBackupCopy = SubmissionFileParser.parse(submissionSummaryFile);
        Submission submission = SubmissionFileParser.parse(submissionSummaryFile);
        submissionUpdater.updateSubmission(submission, project);
        ValidationReport validationReport = SubmissionValidator.validateSubmissionSyntax(submission);
        if (submission.getProjectMetaData().getSubmissionType().equals(SubmissionType.PRIDE) ||
                project.getSubmissionDate().before(PRIDE_3_RELEASE_DATE.getTime()) ||
                !validationReport.hasError()) {
            backupSubmissionSummaryFile(submissionSummaryFile, submissionBackupCopy);
            SubmissionFileWriter.write(submission, submissionSummaryFile);
        } else {
            StringBuilder combinedErrorMessage = combineErrorMessages(validationReport);
            throw new Exception("Submission validation failed before update submission summary file for project: " +
                    project.getAccession() + " \nERROR: " + combinedErrorMessage.toString());
        }
    }

    /**
     * Combines error messages from a ValidationReport.
     * @param validationReport the validation report that contains error messages.
     * @return a StringBuilder that mentions all potential error messaages.
     */
    private StringBuilder combineErrorMessages(ValidationReport validationReport) {
        List<ValidationMessage> messages = validationReport.getMessages();
        StringBuilder combinedErrorMessage = new StringBuilder();
        for (ValidationMessage message : messages) {
            if (message.getType().equals(ValidationMessage.Type.ERROR)) {
                combinedErrorMessage.append(message.getMessage());
            }
        }
        return combinedErrorMessage;
    }

    /**
     * Backs up a submission object to a target file with a timestamp of the format "-yyyyMMdd-hhmmss" appended.
     * @param submissionSummaryFile the original file, of which the backup will be in the same directory
     * @param submissionBackupCopy the submission to be backed up
     * @throws SubmissionFileException any problems writing the new submission.px backup file.
     */
    private void backupSubmissionSummaryFile(File submissionSummaryFile, Submission submissionBackupCopy) throws SubmissionFileException {
        SubmissionFileWriter.write(submissionBackupCopy, new File(submissionSummaryFile.getAbsolutePath() + "-" +
                new SimpleDateFormat("yyyyMMdd-hhmmss").format(Calendar.getInstance().getTime())));
    }
}
