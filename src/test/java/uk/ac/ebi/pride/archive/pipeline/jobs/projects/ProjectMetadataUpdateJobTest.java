package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {PrideProjectMetadataUpdateJob.class, JobRunnerTestConfiguration.class})
@TestPropertySource(value = "classpath:application-test.properties")
@Slf4j

public class ProjectMetadataUpdateJobTest {

    @Autowired
    PrideProjectMetadataUpdateJob prideProjectMetadataUpdateJob;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    /**
     * This test should be run during the development process.
     * WARNNING! the file you are parsing as the parameter will get deleted at the last step of the pipeline.
     * You may backup your original file before running the test case
     */
    @Test
    @Ignore
    public void projectMetadataUpdateJobTest() {

            JobParameters param = new JobParametersBuilder()
                    .addString("instance_id", UUID.randomUUID().toString(), true)
                    .addString("modifiedSubmissionSummaryFile", "/absolute/path/here/submission_edited_version.px")
                    .addString("projectAccession", "PXD010142")
                    .toJobParameters();

            JobExecution jobExecution = null;
            try {
                jobExecution = jobLauncherTestUtils.launchJob(param);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());
    }
}