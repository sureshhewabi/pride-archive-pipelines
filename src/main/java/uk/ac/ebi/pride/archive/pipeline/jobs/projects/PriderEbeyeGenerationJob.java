package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.listeners.PipelineJobStatusListener;
import uk.ac.ebi.pride.archive.pipeline.tasklets.GenerateEbeyeXmlTasklet;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class})
public class PriderEbeyeGenerationJob extends AbstractArchiveJob {

    @Autowired
    private GenerateEbeyeXmlTasklet generateEbeyeXmlTasklet;

    @Bean
    public Job priderEbeyeXmlGenerationJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDER_EBEYE_XML_GENERATION.getName())
                .listener(new PipelineJobStatusListener())
                .start(genEbEyeXmlStep())
                .build();
    }

    private Step genEbEyeXmlStep() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDER_EBEYE_XML_GENERATION.name())
                .tasklet(generateEbeyeXmlTasklet)
                .build();
    }

}
