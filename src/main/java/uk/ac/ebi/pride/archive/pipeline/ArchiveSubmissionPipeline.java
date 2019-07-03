package uk.ac.ebi.pride.archive.pipeline;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
@EnableBatchProcessing
public class ArchiveSubmissionPipeline {

  public static void main(String[] args) {
    ApplicationContext context = SpringApplication.run(ArchiveSubmissionPipeline.class, args);
    System.exit(SpringApplication.exit(context));
  }
}