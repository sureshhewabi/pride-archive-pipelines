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
import org.springframework.context.annotation.PropertySource;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveRedisConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.repos.project.Project;
import uk.ac.ebi.pride.archive.repo.repos.project.ProjectRepository;
import uk.ac.ebi.pride.integration.message.model.impl.PublicationCompletionPayload;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * This Job takes the Data from the OracleDB and Sync into MongoDB. A parameter is needed if the user wants to override the
 * existing projects in the database.
 *
 * Todo: We need to check what happen in case of Transaction error.
 *
 * @author hewapathirana
 */
@Configuration
@Slf4j
@PropertySource("classpath:application.properties")
@Import({ArchiveOracleConfig.class, ArchiveMongoConfig.class, DataSourceConfiguration.class, ArchiveRedisConfig.class})
public class SyncMissingProjectsWithMongoJob extends AbstractArchiveJob{


    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    ProjectRepository oracleProjectRepository;

    @Autowired
    uk.ac.ebi.pride.archive.pipeline.services.redis.RedisMessageNotifier messageNotifier;

    @Value("${archive.post.publication.completion.queue}")
    private String redisQueueName;

    /**
     * Defines the job to Sync all missing projects from OracleDB into MongoDB database.
     *
     * @return the  job
     */
    @Bean
    public Job syncMissingProjectsOracleToMongoJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SYNC_MISSING_PROJECTS_ORACLE_MONGODB.getName())
                .start(syncMissingProjectOracleToMongoDB())
                .build();
    }

    /**
     * This methods connects to the database read all the Oracle information for public
     * @return
     */
    @Bean
    Step syncMissingProjectOracleToMongoDB() {
    return stepBuilderFactory
        .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MISSING_PROJ_ORACLE_TO_MONGO_SYNC.name())
        .tasklet(
            (stepContribution, chunkContext) -> {

              final Set<String> oracleProjectAccessions = getOracleProjectAccessions();
              final Set<String> mongoDBProjectAccessions = getMongoProjectAccessions();

              log.info("Number of projects in Oracle DB: " + oracleProjectAccessions.size());
              log.info("Number of projects in Mongo DB : " + mongoDBProjectAccessions.size());

              Set<String> oracleProjectAccessionsMongoCopy = new HashSet<>();
              Set<String> mongoDBProjectAccessionsCopy = new HashSet<>();

              oracleProjectAccessionsMongoCopy.addAll(oracleProjectAccessions);
              mongoDBProjectAccessionsCopy.addAll(mongoDBProjectAccessions);

              // get list of accessions missing in mongoDB
              oracleProjectAccessionsMongoCopy.removeAll(mongoDBProjectAccessions);
              log.info("List of accessions missing in MongoDB: " + oracleProjectAccessionsMongoCopy.toString());
              notifyToMessagingQueue(oracleProjectAccessionsMongoCopy, true);

              // get list of accessions missing in in Oracle due to reset or mistakenly added to Mongo
              mongoDBProjectAccessionsCopy.removeAll(oracleProjectAccessions);
              log.info("List of accessions mistakenly added to MongoDB: " + mongoDBProjectAccessionsCopy.toString());
              notifyToMessagingQueue(mongoDBProjectAccessionsCopy, false);

              return RepeatStatus.FINISHED;
            })
        .build();
    }

    /**
     * Connect to Oracle Database and get project accessions of all the public projects
     * (project with old PRD accessions)
     *
     * @return Set of project accessions
     */
    private Set<String> getOracleProjectAccessions(){

        Iterable<Project> oracleAllProjects = oracleProjectRepository.findAll();
        Set<String> oracleAccessions = StreamSupport.stream(oracleAllProjects.spliterator(), false)
                .filter(Project::isPublicProject)
                .map(Project::getAccession)
                .collect(Collectors.toSet());

        log.info("Number of Oracle projects: "+ oracleAccessions.size());
        return oracleAccessions;
    }

    /**
     * Connect to MongoDB Database and get project accessions of all the projects
     *
     * @return Set of project accessions
     */
    private Set<String> getMongoProjectAccessions(){

        Set<String> mongoProjectAccessions =  prideProjectMongoService.findAllStream()
                .map(MongoPrideProject::getAccession)
                .collect(Collectors.toSet());
        log.info( "Number of MongoDB projects: "+ mongoProjectAccessions.size());
        return mongoProjectAccessions;
    }

    /**
     * Notify to the archive.post.publication.completion.queue to either insert into or reset from
     * MongoDB and solr
     * @param projects list of project accessions
     * @param isInsert flag to indicate either to insert to reset
     */
    private void notifyToMessagingQueue(Set<String> projects, boolean isInsert){

        for (String project : projects) {
            String message = (isInsert) ? project : project + "_ERROR";
            messageNotifier.sendNotification(redisQueueName,
                    new PublicationCompletionPayload(message),PublicationCompletionPayload.class);
            log.info("Notified to redis queue " + redisQueueName + " : " + message);
        }
    }
}
