package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.repos.file.ProjectFileRepository;
import uk.ac.ebi.pride.archive.repo.repos.project.ProjectRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;


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
@Import({ArchiveOracleConfig.class, ArchiveMongoConfig.class})
public class SyncProjectsOracleToMongoJob extends AbstractArchiveJob {

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

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

    /**
     * This method take the parameter from the override. If this value is True then the MongoDB
     * data will be overrid with the data from the Oracle Database.
     *
     * @param override
     */
    public void setOverride(final String override){
        if(override != null && override.equalsIgnoreCase("TRUE"))
            this.override = true;
        else
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
    Step syncOracleToMongoDB() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC.name())
                .tasklet((stepContribution, chunkContext) -> {
                    setOverride(chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().get("override").toString());
                    log.info(oracleProjectRepository.findAllAccessions().toString());
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
    public Job calculatePrideArchiveDataUsage() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_ORACLE_MONGODB_SYNC.getName())
                .start(syncOracleToMongoDB())
                .build();
    }


}
