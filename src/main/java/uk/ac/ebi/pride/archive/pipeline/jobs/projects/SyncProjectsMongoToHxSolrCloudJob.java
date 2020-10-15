package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.solr.api.client.SolrProjectClient;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class Job sync all the projects from MongoDB to SolrCloud. The first approach would be to index the projects from the
 * MongoDB and then other jobs can be used to index the from Oracle.
 *
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 13/06/2018.
 */
@Configuration
public class SyncProjectsMongoToHxSolrCloudJob extends SyncProjectsMongoToSolrCloudJob {

    public SyncProjectsMongoToHxSolrCloudJob(PrideProjectMongoService prideProjectMongoService,
                                             PrideFileMongoService prideFileMongoService,
                                             @Qualifier("solrProjectClientHX") SolrProjectClient solrProjectClient) {
        super(prideProjectMongoService, prideFileMongoService, solrProjectClient);
    }

    /**
     * This methods connects to the database read all the Mongo information for public
     *
     * @return
     */
    @Bean
    Step syncProjectMongoDBToSolrCloudStepHX() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGO_TO_SOLR_SYNC_HX.name())
                .tasklet(syncProjectMongoToSolrTasklet())
                .build();
    }

    /**
     * Clean all the documents in the SolrCloud Master for Sync
     *
     * @return return Step
     */
    @Bean
    Step cleanSolrCloudStepHX() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_CLEAN_SOLR_HX.name())
                .tasklet(cleanSolrCloudTasklet()).build();
    }

    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job syncMongoProjectToSolrCloudJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_SOLRCLOUD_SYNC_HX.getName())
                .start(cleanSolrCloudStepHX())
                .next(syncProjectMongoDBToSolrCloudStepHX())
//                .next(syncFilesToSolrProjectStep()) //file sync is also included in above step
                .build();

    }


}
