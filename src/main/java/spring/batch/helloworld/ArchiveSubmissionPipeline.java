package spring.batch.helloworld;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class ArchiveSubmissionPipeline {

	public static void main(String[] args) {
		SpringApplication.run(ArchiveSubmissionPipeline.class, args);
	}
  // todo setup repo
  // todo separate repo for bsub launch scripts
}
