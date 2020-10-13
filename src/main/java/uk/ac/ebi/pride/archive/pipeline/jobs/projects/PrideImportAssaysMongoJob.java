package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import uk.ac.ebi.pride.archive.dataprovider.project.SubmissionType;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveRedisConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.RepoConfig;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.client.AssayRepoClient;
import uk.ac.ebi.pride.archive.repo.client.FileRepoClient;
import uk.ac.ebi.pride.archive.repo.client.ProjectRepoClient;
import uk.ac.ebi.pride.archive.repo.models.assay.Assay;
import uk.ac.ebi.pride.archive.repo.models.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.models.project.Project;
import uk.ac.ebi.pride.integration.message.model.impl.AssayDataGenerationPayload;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoPrideAssay;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.repo.files.PrideFileMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
@EnableBatchProcessing
@Slf4j
@PropertySource("classpath:application.properties")
@Import({RepoConfig.class, ArchiveMongoConfig.class, DataSourceConfiguration.class, ArchiveRedisConfig.class})
public class PrideImportAssaysMongoJob extends AbstractArchiveJob {


    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    AssayRepoClient assayRepoClient;

    @Autowired
    ProjectRepoClient projectRepoClient;

    @Autowired
    PrideFileMongoRepository prideFileMongoRepository;

    @Autowired
    FileRepoClient fileRepoClient;

    @Autowired
    RedisConnectionFactory connectionFactory;

    @Autowired
    uk.ac.ebi.pride.archive.pipeline.services.redis.RedisMessageNotifier messageNotifier;

    @Value("${redis.assay.analyse.queue}")
    private String redisQueueName;

    private String projectAccession;

    @Bean
    @StepScope
    public Tasklet importAssayInitJob(@Value("#{jobParameters['project']}") String projectAccession) {
        return (stepContribution, chunkContext) ->
        {
            this.projectAccession = projectAccession;
            System.out.println(String.format("==================>>>>>>> Run the PrideImportAssaysMongoJob job for Project %s", projectAccession));
            return RepeatStatus.FINISHED;
        };
    }

    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job importProjectAssaysInformationJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_ASSAY_SYNC.getName())
                .start(stepBuilderFactory
                        .get("importAssayInitJob")
                        .tasklet(importAssayInitJob(null))
                        .build())
                .next(importProjectAssayInformationStep())
                .build();
    }

    @Bean
    Step importProjectAssayInformationStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SYNC_ASSAY_TO_MONGO.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if (projectAccession != null) {
                        syncProject(projectAccession);
                    } else {
                        projectRepoClient.getAllPublicAccessions().forEach(this::syncProject);
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

    public void syncProject(String projectAccession) {
        try {
            Project project = projectRepoClient.findByAccession(projectAccession);
            if (!project.isPublicProject()) {
                log.warn("This is a private submission, therefore Sync will not happen : " + projectAccession);
                return;
            }
            if (project.getSubmissionType().equals(SubmissionType.PRIDE.name())) {
                log.warn("Sync will not happen as submission type is " + project.getSubmissionType() + " : " + projectAccession);
                return;
            }
            List<Assay> assays = assayRepoClient.findAllByProjectId(project.getId());
            List<ProjectFile> files = fileRepoClient.findAllByProjectId(project.getId());
            doProjectAssaySync(assays, files, project);
            notifyToMessagingQueue(project, assays);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }


    private void doProjectAssaySync(List<Assay> assays, List<ProjectFile> files, Project project) {
        Optional<MongoPrideProject> projectMongo = prideProjectMongoService.findByAccession(project.getAccession());
        List<MongoPrideFile> mongoFiles = prideFileMongoRepository.findByProjectAccessions(Collections.singletonList(project.getAccession()));
        if (projectMongo.isPresent() && mongoFiles != null && mongoFiles.size() > 0) {
            List<MongoPrideAssay> mongoAssays = PrideProjectTransformer.transformOracleAssayToMongo(assays, files, mongoFiles, project);
            prideProjectMongoService.saveAssays(mongoAssays);
            log.info("The assays for project -- " + project.getAccession() + " have been inserted in Mongo");
        } else
            log.error("The project is not present in the Mongo database, please add first the project -- " + project.getAccession());
    }

    /**
     * Notify project accession and assay accession to the redis queue to run the next job which is AssayAnalysisJob
     *
     * @param project Project
     * @param assays  Assay
     */
    private void notifyToMessagingQueue(Project project, List<Assay> assays) {

        assays.forEach(
                assay -> {
                    messageNotifier.sendNotification(redisQueueName,
                            new AssayDataGenerationPayload(project.getAccession(), assay.getAccession()),
                            AssayDataGenerationPayload.class);
                    log.info("Notified to redis queue {} : {} - {} ", redisQueueName, project.getAccession(), assay.getAccession());
                });
    }
}
