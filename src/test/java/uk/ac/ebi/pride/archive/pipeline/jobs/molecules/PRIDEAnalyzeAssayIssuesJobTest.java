package uk.ac.ebi.pride.archive.pipeline.jobs.molecules;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.pride.archive.pipeline.configuration.JobRunnerTestConfiguration;

import java.util.UUID;

/**
 * @author Suresh Hewapathirana
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {PRIDEAnalyzeAssayIssuesJob.class, JobRunnerTestConfiguration.class})
@TestPropertySource(value = "classpath:application-test.properties")
@Slf4j
@Ignore
public class PRIDEAnalyzeAssayIssuesJobTest {

    @Autowired
    PRIDEAnalyzeAssayIssuesJob prideAnalyzeAssayIssuesJob;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void assayAnalysisIssuesTest() throws Exception {
        JobParameters param = new JobParametersBuilder()
                .addString("instance_id", UUID.randomUUID().toString(), true)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(param);
        Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());
    }
}