//package uk.ac.ebi.pride.archive.pipeline.jobs.users;
//
//import lombok.extern.slf4j.Slf4j;
//import org.junit.Assert;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.batch.core.BatchStatus;
//import org.springframework.batch.core.JobExecution;
//import org.springframework.batch.core.JobParameters;
//import org.springframework.batch.core.JobParametersBuilder;
//import org.springframework.batch.test.JobLauncherTestUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
//import org.springframework.security.crypto.password.PasswordEncoder;
//import org.springframework.test.context.ContextConfiguration;
//import org.springframework.test.context.TestPropertySource;
//import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
//import org.springframework.test.util.ReflectionTestUtils;
//import uk.ac.ebi.pride.archive.pipeline.configuration.JobRunnerTestConfiguration;
//import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
//
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(classes = {UserAccountsAAPSyncJob.class, JobRunnerTestConfiguration.class})
//@TestPropertySource(value = "classpath:application-test.properties")
//@Slf4j
//public class UsersAAPSyncJobTest {
//
//    @Autowired
//    UserAccountsAAPSyncJob userAccountsAAPSyncJob;
//
//    @Autowired
//    private JobLauncherTestUtils jobLauncherTestUtils;
//
//    @Test
//    public void syncPrideUsersToAAPDB() throws Exception {
//
//        /*PasswordEncoder encoder = new BCryptPasswordEncoder();
//        System.out.println(encoder.encode("test"));*/
//
//        JobParameters param = new JobParametersBuilder()
//                .toJobParameters();
//        JobExecution jobExecution = jobLauncherTestUtils.launchJob(param);
//        Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());
//    }
//
//}
