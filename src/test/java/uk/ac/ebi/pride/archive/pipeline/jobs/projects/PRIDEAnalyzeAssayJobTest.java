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
import uk.ac.ebi.pride.archive.pipeline.jobs.molecules.PRIDEAnalyzeAssayJob;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {PRIDEAnalyzeAssayJob.class, JobRunnerTestConfiguration.class})
@TestPropertySource(value = "classpath:application-test.properties")
@Slf4j
public class PRIDEAnalyzeAssayJobTest {

    @Autowired
    PRIDEAnalyzeAssayJob prideAnalyzeAssayJob;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    /**
     * This test should be run during the development process
     */
    @Test
    public void analyzeAssayInformationToMongo() {
            JobParameters param = new JobParametersBuilder()
                    .addString("project", "PXD010142")
                    .addString("assay", "93465")
                    .toJobParameters();
            ReflectionTestUtils.setField(prideAnalyzeAssayJob, "projectAccession", "PXD010142");
            ReflectionTestUtils.setField(prideAnalyzeAssayJob,"assayAccession", "93465");
            JobExecution jobExecution = null;
            try {
                jobExecution = jobLauncherTestUtils.launchJob(param);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());
    }


    /**
     * This test should be run during the development process
     * @throws Exception
     */
    @Test
    public void simpleFileImport() throws Exception {
        JobParameters param = new JobParametersBuilder()
                    .addString("project", "PXD010142")
                    .addString("assay", "93466")
                    .toJobParameters();
            ReflectionTestUtils.setField(prideAnalyzeAssayJob, "projectAccession", "PXD010142");
            ReflectionTestUtils.setField(prideAnalyzeAssayJob,"assayAccession", "93466");
            JobExecution jobExecution = null;
            try {
                jobExecution = jobLauncherTestUtils.launchJob(param);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());

    }
}