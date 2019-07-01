package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

import java.util.List;
import java.util.Optional;
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

//    @Autowired
//    SolrTemplate template;

    @Value("${accession:#{null}}")
    private String accession;


    private void doProjectSync(MongoPrideProject mongoPrideProject){
        PrideSolrProject solrProject = PrideProjectTransformer.transformProjectMongoToSolr(mongoPrideProject);
        PrideSolrProject status = solrProjectService.save(solrProject);
        log.info("The project -- " + status.getAccession() + " has been inserted in SolrCloud");
    }

    private void doFilesSync(PrideSolrProject prideSolrProject){
        List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(prideSolrProject.getAccession());
        Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
        prideSolrProject.setProjectFileNames(fileNames);
        PrideSolrProject savedProject = solrProjectService.update(prideSolrProject);
        log.info("The files for project -- " + savedProject.getAccession() + " have been inserted in SolrCloud");
    }

    /**
     * This methods connects to the database read all the Oracle information for public
     * @return
     */
    @Bean
    Step syncProjectMongoDBToSolrCloudStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if(accession != null){
                        Optional<MongoPrideProject> mongoPrideProjectOptional = prideProjectMongoService.findByAccession(accession);
                        if(mongoPrideProjectOptional.isPresent()) {
                            MongoPrideProject mongoPrideProject = mongoPrideProjectOptional.get();
                            doProjectSync(mongoPrideProject);
                        }
                    }else{
                        prideProjectMongoService.findAllStream().forEach( mongoPrideProject ->{
                            doProjectSync(mongoPrideProject);
                        });
                        /*Optional<MongoPrideProject> mongoPrideProjectOptional = prideProjectMongoService.findAllStream().findFirst();
                        if(mongoPrideProjectOptional.isPresent()) {
                            MongoPrideProject mongoPrideProject = mongoPrideProjectOptional.get();
                            doProjectSync(mongoPrideProject);
                        }*/
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * Clean all the documents in the SolrCloud Master for Sync
     * @return return Step
     */
    @Bean
    Step cleanSolrCloudStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_CLEAN_SOLR.name())
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("#####################Accession:"+accession);
                    if(accession!=null){
                        PrideSolrProject prideSolrProject = solrProjectService.findByAccession(accession);
                        if(prideSolrProject != null) {
                            String id = (String) prideSolrProject.getId();
                            solrProjectService.deleteProjectById(id);
                            log.info("Document with id: " + id + " has been deleted from the SolrCloud Master");
                        }
                    }else{
                        //solrProjectService.deleteAll();
                        solrProjectService.findAll().forEach(x-> {
                            solrProjectService.deleteProjectById(x.getAccession());
                        });
                        log.info("All Documents has been deleted from the SolrCloud Master");
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

    /**
     * Sync the Files to Solr Project
     * @return Step
     */
    @Bean
    Step syncFilesToSolrProjectStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SYNC_FILES_TO_PROJECT_SOLR.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if(accession != null){
                        PrideSolrProject prideSolrProject = solrProjectService.findByAccession(accession);
                        doFilesSync(prideSolrProject);
                    }else{
                        solrProjectService.findAll().forEach(this::doFilesSync);
                    }
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
                .start(cleanSolrCloudStep())
                .next(syncProjectMongoDBToSolrCloudStep())
                .next(syncFilesToSolrProjectStep())
                .build();

    }



}
