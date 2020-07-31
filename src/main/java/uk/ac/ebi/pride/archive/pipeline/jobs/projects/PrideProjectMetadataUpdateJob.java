package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import uk.ac.ebi.pride.archive.dataprovider.file.ProjectFileSource;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.ProteomeCentralConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.RepoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.SubmissionToProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.tasklets.GenerateEbeyeXmlTasklet;
import uk.ac.ebi.pride.archive.pipeline.tasklets.PxXmlUpdater;
import uk.ac.ebi.pride.archive.pipeline.tasklets.SubmissionSummaryFileUpdater;
import uk.ac.ebi.pride.archive.pipeline.tasklets.SubmissionUpdater;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.client.CvParamRepoClient;
import uk.ac.ebi.pride.archive.repo.client.ProjectRepoClient;
import uk.ac.ebi.pride.archive.repo.models.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.models.project.*;
import uk.ac.ebi.pride.archive.repo.util.ObjectMapper;
import uk.ac.ebi.pride.archive.utils.config.FilePathBuilder;
import uk.ac.ebi.pride.archive.utils.streaming.FileUtils;
import uk.ac.ebi.pride.data.exception.SubmissionFileException;
import uk.ac.ebi.pride.data.io.SubmissionFileParser;
import uk.ac.ebi.pride.data.model.Submission;
import uk.ac.ebi.pride.data.validation.SubmissionValidator;
import uk.ac.ebi.pride.data.validation.ValidationMessage;
import uk.ac.ebi.pride.data.validation.ValidationReport;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
@Slf4j
@EnableBatchProcessing
@ComponentScan({"uk.ac.ebi.pride.archive.pipeline.tasklets"})
@Import({RepoConfig.class, ArchiveMongoConfig.class, DataSourceConfiguration.class, ArchiveMongoConfig.class, SolrCloudMasterConfig.class, ProteomeCentralConfig.class})
public class PrideProjectMetadataUpdateJob extends AbstractArchiveJob {

    @Value("${pride.data.prod.directory}")
    String prePublicDatasetDir;

    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    SolrProjectService solrProjectService;

    @Autowired
    ProjectRepoClient projectRepoClient;

    @Autowired
    CvParamRepoClient cvParamRepoClient;

    @Autowired
    FileUtils fileUtils;

    @Autowired
    FilePathBuilder filePathBuilder;

    @Autowired
    PxXmlUpdater pxXmlUpdater;

    @Autowired
    GenerateEbeyeXmlTasklet generateEbeyeXmlTasklet;

    @Autowired
    SubmissionSummaryFileUpdater submissionSummaryFileUpdater;

    @Autowired
    PrideFileMongoService prideFileMongoService;

    Project modifiedProject;
    String modifiedSubmissionSummaryFile;

    @Bean
    SubmissionUpdater submissionUpdater(){
        return new SubmissionUpdater();
    }

    @Bean
    SubmissionSummaryFileUpdater submissionSummaryFileUpdater(SubmissionUpdater submissionUpdater){
        return new SubmissionSummaryFileUpdater(submissionUpdater);
    }

    @Bean
    @StepScope
    public Tasklet initJobProjectMetadataUpdateJob(@Value("#{jobParameters['projectAccession']}") String projectAccession, @Value("#{jobParameters['modifiedSubmissionSummaryFile']}") String modifiedSubmissionSummaryFile) {
        return (stepContribution, chunkContext) ->
        {
            System.out.println(String.format("==================>>>>>>> ProjectMetadataUpdateJob - Run the job for Project ", projectAccession));

            // check the input summary file
            this.modifiedSubmissionSummaryFile = modifiedSubmissionSummaryFile;
            Submission submission;
            try {
                File inputFile = new File(modifiedSubmissionSummaryFile);
                if(inputFile.exists()) {
                    submission = SubmissionFileParser.parse(inputFile);
                }else{
                    throw new Exception("Input summary.px file does not exist in " + modifiedSubmissionSummaryFile);
                }
            } catch (SubmissionFileException e) {
                throw new Exception("Submission file parsing failed" + e.getMessage());
            }

            // validate the summary file
            ValidationReport validationReport = SubmissionValidator.validateSubmissionSyntax(submission);
            if (!validationReport.hasError()) {
                // copy values from submission.px file to current Project object
                this.modifiedProject = projectRepoClient.findByAccession(projectAccession);
                SubmissionToProjectTransformer submissionToProjectTransformer = new SubmissionToProjectTransformer(cvParamRepoClient);
                this.modifiedProject = submissionToProjectTransformer.transform(submission, modifiedProject);
            } else {
                throw new Exception("Submission validation failed. ERROR: " + String.join(",", validationReport.getMessages().stream().filter(validationMessage -> validationMessage.getType().equals(ValidationMessage.Type.ERROR)).map(ValidationMessage::getMessage).collect(Collectors.toList())));
            }
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step updateProjectMetadataStep() {

        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_UPDATE_PROJECT_METADATA.name())
                .tasklet((stepContribution, chunkContext) -> {
                    try {
                        // 1. change in the Oracle database
                        projectRepoClient.save(modifiedProject);

                        // 2.  Update submission.px file
                        ProjectSummary projectSummary = ObjectMapper.mapProjectToProjectSummary(modifiedProject);
                        ProjectFile submissionSummaryPXFile = new ProjectFile();
                        submissionSummaryPXFile.setFileName("submission.px");
                        submissionSummaryPXFile.setFileSource(ProjectFileSource.INTERNAL);
                        String filePath = filePathBuilder.buildPublicationFilePath(prePublicDatasetDir, modifiedProject, submissionSummaryPXFile);
                        File submissionSummaryFile = fileUtils.findFileToStream(filePath);
                        submissionSummaryFileUpdater.updateSubmissionSummaryFile(projectSummary, submissionSummaryFile);

                        // 3. Regenerate/update PX XML and send it to ProteomeCentral
                        pxXmlUpdater.updatePXXml(projectSummary, submissionSummaryFile);

                        if (modifiedProject.isPublicProject()) {
                            // 4. Insert into new MongoDB Database
                                 Optional<MongoPrideProject> mongoExistingProject = prideProjectMongoService.findByAccession(modifiedProject.getAccession());
                                if(mongoExistingProject.isPresent()) {
                                    MongoPrideProject mongoPrideProject = PrideProjectTransformer.transformOracleToMongo(modifiedProject);
                                    mongoPrideProject.setId((ObjectId) (mongoExistingProject.get()).getId());
                                    Optional<MongoPrideProject> mongoUpdatedProject = prideProjectMongoService.update(mongoPrideProject);
                                    if(mongoUpdatedProject.isPresent()) {
                                        log.info("The project -- " + mongoUpdatedProject.get().getAccession() + " has been updated in MongoDB");

                                        // 5.  Reindex the project in the PRIDE Archive's Solr project index
                                        PrideSolrProject solrPrideProject = PrideProjectTransformer.transformProjectMongoToSolr(mongoUpdatedProject.get());
                                        List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(mongoUpdatedProject.get().getAccession());
                                        Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
                                        solrPrideProject.setProjectFileNames(fileNames);
                                        PrideSolrProject solrExistingProject = solrProjectService.findByAccession(modifiedProject.getAccession());
                                        solrPrideProject.setId(solrExistingProject.getId().toString());
                                        PrideSolrProject solrModifiedProject = solrProjectService.update(solrPrideProject);
                                        if(solrModifiedProject != null) {
                                            log.info("The project -- " + solrModifiedProject.getAccession() + " has been inserted in SolrCloud");
                                        }else{
                                            log.error(modifiedProject.getAccession() + " does not updated");
                                        }
                                    }
                                } else {
                                    log.error(modifiedProject.getAccession() + " does not updated");
                                }

                                // 6. Regenerate EBeye XML and output it to the PRIDE ARchive EBeye directory
                                generateEbeyeXmlTasklet.generateEBeyeXml(modifiedProject.getAccession());
                        } else {
                            log.info(("Project is private, not reindexing or generating EBeye XML or update mongoDB or Solr"));
                        }
                    } catch (Exception e) {
                        log.error("An error occurred while syncing :" + e.getMessage());
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

    /**
     * Summary input file will be deleted from the file system
     * @return
     */
    @Bean
    public Step deleteTempSubmissionFileJob() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_DELETE_TEMP_SUMMARY_FILE.name())
                .tasklet((stepContribution, chunkContext) -> {
                    try {
                        Files.deleteIfExists(Paths.get(modifiedSubmissionSummaryFile));
                        log.info("Input summary.px file deleted : " + modifiedSubmissionSummaryFile);
                    } catch(NoSuchFileException e) {
                        log.error("Input summary.px file does not exist to delete : " + modifiedSubmissionSummaryFile);
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

    /**
     * This job updates the project metadata in several resources as all the databases(mongoDB, Oracle), Solr index,
     * and other files
     * @return
     */
    @Bean
    public Job projectMetadataUpdateJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_PROJECT_METADATA_UPDATE.getName())
                .start(stepBuilderFactory
                        .get("initJobProjectMetadataUpdateJob")
                        .tasklet(initJobProjectMetadataUpdateJob(null, null))
                        .build())
                .next(updateProjectMetadataStep())
                .next(deleteTempSubmissionFileJob())
                .build();
    }
}
