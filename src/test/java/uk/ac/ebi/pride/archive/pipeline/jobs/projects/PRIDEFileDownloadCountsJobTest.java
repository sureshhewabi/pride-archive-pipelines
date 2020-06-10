package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
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
import org.springframework.test.util.ReflectionTestUtils;
import uk.ac.ebi.pride.archive.pipeline.configuration.JobRunnerTestConfiguration;

import java.util.UUID;

/**
 * @author Suresh Hewapathirana
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {PrideFileDownloadCountsJob.class, JobRunnerTestConfiguration.class})
@TestPropertySource(value = "classpath:application-test.properties")
@Slf4j
public class PRIDEFileDownloadCountsJobTest {

    @Autowired
    PrideFileDownloadCountsJob prideFileDownloadCountsJob;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    /**
     * This test should be run during the development process
     * @throws Exception
     */
    @Test
    public void fileDownloadCountsJobTest() throws Exception {
        final String startDate = "2020-05-28";
        final String endDate   = "2020-05-29";

        JobParameters param = new JobParametersBuilder()
                .addString("instance_id", UUID.randomUUID().toString(), true)
                .addString("startDate", startDate)
                .addString("endDate", endDate)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(param);
        Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());
    }
}
