package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;
import uk.ac.ebi.pride.archive.pipeline.configuration.JobRunnerTestConfiguration;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 14/06/2018.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SyncProjectsMongoToHxSolrCloudJob.class, JobRunnerTestConfiguration.class})
@TestPropertySource(value = "classpath:application-test.properties")
@Slf4j
public class SyncProjectsMongoToHxSolrCloudJobTest {

        @Autowired
        SyncProjectsMongoToHxSolrCloudJob mongoToSolrCloudJob;

        @Autowired
        private JobLauncherTestUtils jobLauncherTestUtils;

        /**
         * This test should be run during the development process
         * @throws Exception
         */
        @Test
        public void syncMongoToSolrCloud() throws Exception {

            ReflectionTestUtils.setField(mongoToSolrCloudJob, "accession", "PXD008498");

            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());
        }
}
