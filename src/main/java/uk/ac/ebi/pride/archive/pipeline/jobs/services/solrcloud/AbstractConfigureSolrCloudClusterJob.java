package uk.ac.ebi.pride.archive.pipeline.jobs.services.solrcloud;

import jdk.nashorn.internal.ir.annotations.Ignore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.solr.commons.PrideProjectField;
import uk.ac.ebi.pride.solr.commons.SolrAPIHelper;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 22/06/2018.
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@EnableConfigurationProperties
public abstract class AbstractConfigureSolrCloudClusterJob extends AbstractArchiveJob {

    private SolrAPIHelper solrAPIHelper;

    /**
     * Defines the job to calculate and collate PRIDE Archive data usage.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job createPrideArchiveSolrCloudCollectionJob() {
        this.solrAPIHelper = SolrAPIHelper.getInstance(getSolrMasterURL());
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SOLR_MASTER_INIT.getName())
                .start(deletePrideArchiveCollectionSolrCloudStep())
                .next(createPrideArchiveCollectionSolrCloudStep())
//                .next(refineArchiveCollectionSolrCloud())
                .build();
    }

    /**
     * This method/step connects to the database read all the Oracle information for public
     * @return org.springframework.batch.core.Step
     */
    @Ignore
    private Step deletePrideArchiveCollectionSolrCloudStep() {
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
    @Ignore
    private Step createPrideArchiveCollectionSolrCloudStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SOLR_CLOUD_CREATE_COLLECTION.name())
                .tasklet((stepContribution, chunkContext) -> {
                    String value = (chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("deleteOnly"));
                    if(value != null && !value.equalsIgnoreCase("TRUE")){
                        if(solrAPIHelper.createCollection(PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME, 2, 1, 2,PrideProjectField.PRIDE_PROJECTS_CONFIG_NAME)){
                            log.info("Collection -- " + PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME + " has been create -- ");
                        }
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * Refine the PRIDE Archive Collection.
     * @return org.springframework.batch.core.Step
     */
    private Step refineArchiveCollectionSolrCloudStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SOLR_CLOUD_REFINE_COLLECTION.name())
                .tasklet((stepContribution, chunkContext) -> {
                    String value = (chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("deleteOnly"));
                    if(value != null && !value.equalsIgnoreCase("TRUE")){
                        if(solrAPIHelper.refinePrideSolrProjectsSchema(PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME)){
                            log.info("Collection -- " + PrideProjectField.PRIDE_PROJECTS_COLLECTION_NAME + " has been create -- ");
                        }
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    abstract String getSolrMasterURL();
}
