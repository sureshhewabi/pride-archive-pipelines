package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.tasklets.ForceRunDecider;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;

@Configuration
public class PriderEbeyeGenerationJob extends AbstractArchiveJob {

    @Autowired
    private ForceRunDecider forceRunDecider;


    @Bean
    public Job ebeyeXmlJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDER_EBEYE_XML_GENERATION.getName())
                .start(ebeyeAllDecision())
                .on(ForceRunDecider.YES).to(genEbEyeXmlStep())
                .from(ebeyeAllDecision()).on(ForceRunDecider.NO).to(getAndStoreOrigPublicationDateTaskletStep())
                .end()
                .build();
    }

    private Step getAndStoreOrigPublicationDateTaskletStep() {

        return null;
    }

    private Step genEbEyeXmlStep() {

        return null;
    }

    private Step ebeyeAllDecision() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDER_EBEYE_ALL_DECISION.name())
                .tasklet(forceRunDecider)
                .build();
    }

}
