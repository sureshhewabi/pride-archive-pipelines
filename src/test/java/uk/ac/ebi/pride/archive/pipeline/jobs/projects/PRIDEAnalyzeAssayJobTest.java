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

import java.util.stream.IntStream;

import static org.junit.Assert.*;

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
        IntStream.range(51876, 51940).forEach( x -> {
            JobParameters param = new JobParametersBuilder()
                    .addString("projectAccession", "PXD002089")
                    .addString("assayAccession", String.valueOf(x))
                    .toJobParameters();
//        ReflectionTestUtils.setField(prideAnalyzeAssayJob, "projectAccession", "PXD000001");
//        ReflectionTestUtils.setField(prideAnalyzeAssayJob,"assayAccession", "22134");
            ReflectionTestUtils.setField(prideAnalyzeAssayJob, "projectAccession", "PXD002089");
            ReflectionTestUtils.setField(prideAnalyzeAssayJob,"assayAccession", String.valueOf(x));
//                 ReflectionTestUtils.setField(prideAnalyzeAssayJob, "projectAccession", "PXD001072");
//         ReflectionTestUtils.setField(prideAnalyzeAssayJob,"assayAccession", "37786");

            JobExecution jobExecution = null;
            try {
                jobExecution = jobLauncherTestUtils.launchJob(param);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());
        });

    }


    /**
     * This test should be run during the development process
     * @throws Exception
     */
    @Test
    public void simpleFileImport() throws Exception {
        JobParameters param = new JobParametersBuilder()
                    .addString("projectAccession", "PXD011181")
                    .addString("assayAccession", "99258")
                    .toJobParameters();
            ReflectionTestUtils.setField(prideAnalyzeAssayJob, "projectAccession", "PXD011181");
            ReflectionTestUtils.setField(prideAnalyzeAssayJob,"assayAccession", "99258");
            JobExecution jobExecution = null;
            try {
                jobExecution = jobLauncherTestUtils.launchJob(param);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());

    }
}


// ReflectionTestUtils.setField(prideAnalyzeAssayJob, "projectAccession", "PXD002089");
//         ReflectionTestUtils.setField(prideAnalyzeAssayJob,"assayAccession", "51885");