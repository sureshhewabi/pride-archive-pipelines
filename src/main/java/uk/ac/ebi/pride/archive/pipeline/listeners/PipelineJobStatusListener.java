package uk.ac.ebi.pride.archive.pipeline.listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;

import java.util.List;
import java.util.Map;


@Slf4j
public class PipelineJobStatusListener implements JobExecutionListener {

    public static final String OUTPUT_DIVIDER = "===================================================================================";

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info(OUTPUT_DIVIDER);
        log.info("About to run " + jobExecution.getJobInstance().getJobName());

        log.info("Input job parameters are: ");
        JobParameters parameters = jobExecution.getJobParameters();
        Map<String, JobParameter> parameterMap = parameters.getParameters();
        for (String s : parameterMap.keySet()) {
            log.info(s + " = " + parameterMap.get(s).getValue());
        }
        log.info(OUTPUT_DIVIDER);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info(OUTPUT_DIVIDER);
        log.info("Job exit status: " + jobExecution.getExitStatus().getExitCode());
        List<Throwable> exceptions = jobExecution.getFailureExceptions();
        if (!exceptions.isEmpty()) {
            log.error("Number of exceptions " + exceptions.size());
            for (Throwable exception : exceptions) {
                StackTraceElement[] stackTraceElements = exception.getStackTrace();
                for (StackTraceElement stackTraceElement : stackTraceElements) {
                    log.error(stackTraceElement.toString());
                }
            }
        }
        log.info(OUTPUT_DIVIDER);
    }
}
