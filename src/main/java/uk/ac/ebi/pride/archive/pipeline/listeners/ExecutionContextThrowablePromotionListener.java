package uk.ac.ebi.pride.archive.pipeline.listeners;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import java.util.List;

public class ExecutionContextThrowablePromotionListener extends StepExecutionListenerSupport {

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        JobExecution jobExecution = stepExecution.getJobExecution();

        List<Throwable> exceptions = stepExecution.getFailureExceptions();
        if (!exceptions.isEmpty()) {
            for (Throwable exception : exceptions) {
                jobExecution.addFailureException(exception);
            }
        }

        return ExitStatus.COMPLETED;
    }
}
