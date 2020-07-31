package uk.ac.ebi.pride.archive.pipeline.exceptions;

/**
 * Bespoke exception while updating a submission.
 */
public class SubmissionUpdateException extends RuntimeException {

    /**
     * Default constructor.
     */
    public SubmissionUpdateException() {
    }

    /**
     * Constructor setting the exception message
     * @param message the exception message.
     */
    public SubmissionUpdateException(String message) {
        super(message);
    }

    /**
     * Constructor setting the exception message and cause.
     * @param message the exception message.
     * @param cause the exception cause.
     */
    public SubmissionUpdateException(String message, Throwable cause) {
        super(message, cause);
    }
}
