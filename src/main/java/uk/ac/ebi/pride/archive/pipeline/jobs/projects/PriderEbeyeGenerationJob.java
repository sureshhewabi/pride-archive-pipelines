package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.listeners.ExecutionContextThrowablePromotionListener;
import uk.ac.ebi.pride.archive.pipeline.listeners.PipelineJobStatusListener;
import uk.ac.ebi.pride.archive.pipeline.tasklets.*;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.service.molecules.PrideMoleculesMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class})
public class PriderEbeyeGenerationJob extends AbstractArchiveJob {

    @Autowired
    private ForceRunDecider forceRunDecider;

    @Autowired
    private GenerateEbeyeXmlTasklet generateEbeyeXmlTasklet;

    @Autowired
    private GetAndStoreOrigPublicationDateTasklet getAndStoreOrigPublicationDateTasklet;


    @Bean
    public Job priderEbeyeXmlGenerationJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDER_EBEYE_XML_GENERATION.getName())
                .listener(new PipelineJobStatusListener())
                .start(ebeyeAllDecision())
                .on(ForceRunDecider.YES).to(genEbEyeXmlStep())
                .from(ebeyeAllDecision()).on(ForceRunDecider.NO).to(getAndStoreOrigPublicationDateTaskletStep())
                .end()
                .build();
    }

    private Step getAndStoreOrigPublicationDateTaskletStep() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDER_GET_AND_STORE_ORIGINAL_PUBLICATION.name())
                .listener(executionContextPromotionListener())
                .listener(executionContextThrowablePromotionListener())
                .tasklet(getAndStoreOrigPublicationDateTasklet)
                .build();
    }

    private Step genEbEyeXmlStep() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDER_EBEYE_XML_GENERATION.name())
                .listener(executionContextPromotionListener())
                .listener(executionContextThrowablePromotionListener())
                .tasklet(generateEbeyeXmlTasklet)
                .build();
    }

    private Step ebeyeAllDecision() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDER_EBEYE_ALL_DECISION.name())
                .tasklet(forceRunDecider)
                .build();
    }

    private ExecutionContextPromotionListener executionContextPromotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener = new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(new String[]{"public.path.fragment"});
        executionContextPromotionListener.setStrict(false);
        return executionContextPromotionListener;
    }

    private ExecutionContextThrowablePromotionListener executionContextThrowablePromotionListener() {
        return new ExecutionContextThrowablePromotionListener();
    }


}
