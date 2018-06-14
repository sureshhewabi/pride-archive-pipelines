package uk.ac.ebi.pride.archive.pipeline.jobs.services.solrcloud;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideProjectField;
import uk.ac.ebi.pride.solr.indexes.pride.utils.SolrAPIHelper;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 *
 * This class Configure the PRIDE Archive SolrCloud instance, creating the collection, deleting existing collection,
 * creating the Archive Collection and Refining it.
 *
 * <p>
 * @author ypriverol
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@EnableConfigurationProperties
public class PrideArchiveSolrCloud extends AbstractArchiveJob{

    @Value("${solr.master.url}")
    private String solrMasterURL;

    private SolrAPIHelper solrAPIHelper;

    /**
     * Defines the job to calculate and collate PRIDE Archive data usage.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job createPrideArchiveSolrCloudCollection() {
        this.solrAPIHelper = SolrAPIHelper.getInstance(solrMasterURL);
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SOLR_CLOUD_INIT.getName())
                .start(deletePRIDEArchiveCollectionSolrCloud())
                .next(createArchiveCollectionSolrCloud())
                .next(refineArchiveCollectionSolrCloud())
                .build();
    }

    /**
     * This method/step connects to the database read all the Oracle information for public
     * @return org.springframework.batch.core.Step
     */
    private Step deletePRIDEArchiveCollectionSolrCloud() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SOLR_CLOUD_DELETE_COLLECTION.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if(solrAPIHelper.deleteCollection(PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME)){
                        log.info("Collection -- " + PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME + " has been deleted -- ");
                    }
                    if(solrAPIHelper.deleteConfigSet(PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME)){
                        System.out.println("ConfigSet -- " + PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME + " has been deleted -- ");
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /*
     * This method/step create a Collection in the solrcloud
     * @return org.springframework.batch.core.Step
     */
    private Step createArchiveCollectionSolrCloud() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SOLR_CLOUD_CREATE_COLLECTION.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if(solrAPIHelper.createCollection(PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME, 2, 2, 2)){
                        log.info("Collection -- " + PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME + " has been create -- ");
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * Refine the PRIDE Archive Collection.
     * @return org.springframework.batch.core.Step
     */
    private Step refineArchiveCollectionSolrCloud() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SOLR_CLOUD_REFINE_COLLECTION.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if(solrAPIHelper.refinePrideSolrProjectsSchema(PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME)){
                        log.info("Collection -- " + PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME + " has been create -- ");
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }
}