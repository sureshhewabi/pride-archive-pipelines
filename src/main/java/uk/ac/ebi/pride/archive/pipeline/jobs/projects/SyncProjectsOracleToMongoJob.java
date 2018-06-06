package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;

/**
 * This Job takes the Data from the OracleDB and Sync into MongoDB. A parameter is needed if the user wants to override the
 * existing projects in the database.
 *
 * @author ypriverol
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveOracleConfig.class, ArchiveMongoConfig.class})
public class SyncProjectsOracleToMongoJob {

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

    /**
     * This method take the parameter from the override. If this value is True then the MongoDB
     * data will be overrid with the data from the Oracle Database.
     *
     * @param override
     */
    @Value("#{jobParameters['override']}")
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
    @Value("#{jobParameters['submissionType']}")
    public void setSubmissionType (final String submissionType){
        if(submissionType == null || SubmissionPipelineConstants.SubmissionsType.valueOf(submissionType) == null)
            this.submissionType = SubmissionPipelineConstants.SubmissionsType.PUBLIC;
        else
            this.submissionType = SubmissionPipelineConstants.SubmissionsType.valueOf(submissionType);
    }

    /**
     * Defines the job to calculate and collate PRIDE Archive data usage.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job calculatePrideArchiveDataUsage() {
        return jobBuilderFactory
                .get("SyncOracleToMongoDB")
                .start(syncOracleToMongoDB())
                .build();
    }

    /**
     * This methos connects to the database read all the Oracle information for public
     * @return
     */
    private Step syncOracleToMongoDB() {
        return stepBuilderFactory
                .get("createJedisCluster")
                .tasklet(null)
                .build();

    }


}
