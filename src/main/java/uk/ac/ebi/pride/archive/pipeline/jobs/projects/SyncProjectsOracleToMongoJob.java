package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.repos.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.repos.file.ProjectFileRepository;
import uk.ac.ebi.pride.archive.repo.repos.project.*;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.utilities.util.Tuple;

import java.util.List;
import java.util.Optional;


/**
 * This Job takes the Data from the OracleDB and Sync into MongoDB. A parameter is needed if the user wants to override the
 * existing projects in the database.
 *
 * Todo: We need to check what happen in case of Transaction error.
 *
 * @author ypriverol
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveOracleConfig.class, ArchiveMongoConfig.class, DataSourceConfiguration.class})
public class SyncProjectsOracleToMongoJob extends AbstractArchiveJob{

    // This parameter is use to override all the data in the MongoDB
    private Boolean override;

    // This parameter specify which data will be sync, if is True, only
    private SubmissionPipelineConstants.SubmissionsType submissionType;

    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    ProjectFileRepository oracleFileRepository;

    @Autowired
    ProjectRepository oracleProjectRepository;

    @Value("${ftp.protocol.url}")
    private String ftpProtocol;

    @Value("${aspera.protocol.url}")
    private String asperaProtocol;


    /**
     * This method take the parameter from the override. If this value is True then the MongoDB
     * data will be overrid with the data from the Oracle Database.
     *
     * @param override
     */
    public void setOverride(final String override){
        if(override != null && override.equalsIgnoreCase("TRUE")) {
            this.override = true;
        } else
            this.override = false;
    }

    /**
     * The submission type enables to know which data will be submitted to MongoDB
     * @param submissionType The type of Submissions will be push into MongoDb {@link uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants.SubmissionsType}
     */
    public void setSubmissionType (final String submissionType){
        if(submissionType == null || SubmissionPipelineConstants.SubmissionsType.valueOf(submissionType) == null)
            this.submissionType = SubmissionPipelineConstants.SubmissionsType.PUBLIC;
        else
            this.submissionType = SubmissionPipelineConstants.SubmissionsType.valueOf(submissionType);
    }

    /**
     * This methods connects to the database read all the Oracle information for public
     * @return
     */
    @Bean
    Step syncProjectOracleToMongoDB() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC.name())
                .tasklet((stepContribution, chunkContext) -> {
                    setOverride(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("override"));
                    setSubmissionType(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("submissionType"));
                    oracleProjectRepository.findAll().forEach( oracleProject ->{
                        if(!(!oracleProject.isPublicProject() && (submissionType == SubmissionPipelineConstants.SubmissionsType.PUBLIC))){
                            MongoPrideProject mongoPrideProject = PrideProjectTransformer.transformOracleToMongo(oracleProject);
                            Optional<MongoPrideProject> status = prideProjectMongoService.save(mongoPrideProject);
                            log.info(oracleProject.getAccession() + "-- Inserted Status " + String.valueOf(status.isPresent()));
                        }
                        });
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * The Files will be mapped only for the Projects that has been already sync into MongoDB.
     *
     * @return Step
     */
    @Bean
    public Step syncFileInformationToMongoDB() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC_FILES.name())
                .tasklet((stepContribution, chunkContext) -> {
                    prideProjectMongoService.findAllStream().forEach(mongoPrideProject -> {
                        Project oracleProject = oracleProjectRepository.findByAccession(mongoPrideProject.getAccession());
                        List<ProjectFile> oracleFiles = oracleFileRepository.findAllByProjectId(oracleProject.getId());
                        List<Tuple<MongoPrideFile, MongoPrideFile>> status = prideFileMongoService.insertAll(PrideProjectTransformer.transformOracleFilesToMongoFiles(oracleFiles, oracleProject,ftpProtocol, asperaProtocol));
                        log.info("The following files has been inserted -- " + status.toString());
                        });
                    return RepeatStatus.FINISHED;
                })
                .build();


    }


    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job syncOracleToMongoProjectsJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_ORACLE_MONGODB_SYNC.getName())
                .start(syncProjectOracleToMongoDB())
                .next(syncFileInformationToMongoDB())
                .build();
    }

}
