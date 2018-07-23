package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
@Slf4j
@Import({ArchiveMongoConfig.class, SolrCloudMasterConfig.class, DataSourceConfiguration.class})
public class SyncProjectsMongoToSolrCloudJob extends AbstractArchiveJob {


    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    SolrProjectService solrProjectService;

    /**
     * This methods connects to the database read all the Oracle information for public
     * @return
     */
    @Bean
    Step syncProjectMongoDBToSolrCloudStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC.name())
                .tasklet((stepContribution, chunkContext) -> {
                    prideProjectMongoService.findAllStream().forEach( mongoPrideProject ->{
                            PrideSolrProject solrProject = PrideProjectTransformer.transformProjectMongoToSolr(mongoPrideProject);
                            PrideSolrProject status = solrProjectService.save(solrProject);
                            log.info("The project -- " + status.getAccession() + " has been inserted in SolrCloud");

                    });
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * Clean all the documents in the SolrCloud Master for Sync
     * @return return Step
     */
    Step cleanSolrCloud() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_CLEAN_SOLR.name())
                .tasklet((stepContribution, chunkContext) -> {
                    solrProjectService.deleteAll();
                    log.info("All Documents has been deleted from the SolrCloud Master");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    /**
     * Sync the Files to Solr Project
     * @return Step
     */
    private Step syncFilesToSolrProject() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SYNC_FILES_TO_PROJECT_SOLR.name())
                .tasklet((stepContribution, chunkContext) -> {
                    solrProjectService.findAll().forEach( x-> {
                        List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(x.getAccession());
                        Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
                        x.setProjectFileNames(fileNames);
                        solrProjectService.update(x);
                    });
                    log.info("All Documents has been deleted from the SolrCloud Master");
                    return RepeatStatus.FINISHED;
                }).build();
    }


    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job syncMongoProjectToSolrCloudJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_SOLRCLOUD_SYNC.getName())
                .start(cleanSolrCloud())
                .next(syncProjectMongoDBToSolrCloudStep())
                .next(syncFilesToSolrProject())
                .build();
    }



}
