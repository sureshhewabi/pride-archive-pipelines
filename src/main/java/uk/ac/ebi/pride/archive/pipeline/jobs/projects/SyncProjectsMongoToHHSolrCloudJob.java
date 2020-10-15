package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.solr.api.client.SolrProjectClient;

@Configuration
public class SyncProjectsMongoToHHSolrCloudJob extends SyncProjectsMongoToSolrCloudJob {

    @Autowired
    public SyncProjectsMongoToHHSolrCloudJob(PrideProjectMongoService prideProjectMongoService,
                                             PrideFileMongoService prideFileMongoService,
                                             @Qualifier("solrProjectClientHH") SolrProjectClient solrProjectClient) {
        super(prideProjectMongoService, prideFileMongoService, solrProjectClient);
    }

    @Bean
    Step syncProjectMongoDBToSolrCloudStepHH() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGO_TO_SOLR_SYNC_HH.name())
                .tasklet(syncProjectMongoToSolrTasklet())
                .build();
    }


    @Bean
    Step cleanSolrCloudStepHH() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_CLEAN_SOLR_HH.name())
                .tasklet(cleanSolrCloudTasklet()).build();
    }

    @Bean
    public Job syncMongoProjectToSolrCloudJobHH() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_SOLRCLOUD_SYNC_HH.getName())
                .start(cleanSolrCloudStepHH())
                .next(syncProjectMongoDBToSolrCloudStepHH())
                .build();

    }
}
