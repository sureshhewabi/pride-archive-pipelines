package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class to setup the helper JobLauncherTestUtils for unit testing jobs and steps.
 */
@Configuration
@EnableBatchProcessing
public class JobRunnerConfiguration {

  /**
   * Sets up the JobLauncherTestUtils for uniting jobs and steps.
   * @return the JobLauncherTestUtils
   */
  @Bean
  public JobLauncherTestUtils utils() {
	return new JobLauncherTestUtils();
  }
}