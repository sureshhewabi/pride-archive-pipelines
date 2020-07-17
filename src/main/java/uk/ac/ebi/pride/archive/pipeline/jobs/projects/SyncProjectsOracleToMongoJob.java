package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.RepoConfig;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.client.FileRepoClient;
import uk.ac.ebi.pride.archive.repo.client.ProjectRepoClient;
import uk.ac.ebi.pride.archive.repo.models.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.models.project.Project;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.msrun.MongoPrideMSRun;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 * This Job takes the Data from the OracleDB and Sync into MongoDB. A parameter is needed if the user wants to override the
 * existing projects in the database.
 * <p>
 * Todo: We need to check what happen in case of Transaction error.
 *
 * @author ypriverol
 */
@Configuration
@Slf4j
//@EnableBatchProcessing
@Import({RepoConfig.class, ArchiveMongoConfig.class, DataSourceConfiguration.class})
public class SyncProjectsOracleToMongoJob extends AbstractArchiveJob {

    // This parameter is use to override all the data in the MongoDB
    private Boolean override;

    // This parameter specify which data will be sync, if is True, only
    private SubmissionPipelineConstants.SubmissionsType submissionType;

    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    FileRepoClient fileRepoClient;

    @Autowired
    ProjectRepoClient projectRepoClient;

    @Value("${ftp.protocol.url}")
    private String ftpProtocol;

    @Value("${aspera.protocol.url}")
    private String asperaProtocol;

    @Value("${accession:#{null}}")
    @StepScope
    private String accession;

    @Value("${skipfiles:#{null}}")
    @StepScope
    private Boolean skipFiles;


    /**
     * This method take the parameter from the override. If this value is True then the MongoDB
     * data will be overrid with the data from the Oracle Database.
     *
     * @param override
     */
    public void setOverride(final String override) {
        this.override = override != null && override.equalsIgnoreCase("TRUE");
    }

    /**
     * The submission type enables to know which data will be submitted to MongoDB
     *
     * @param submissionType The type of Submissions will be push into MongoDb {@link uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants.SubmissionsType}
     */
    public void setSubmissionType(final String submissionType) {
        if (submissionType == null)
            this.submissionType = SubmissionPipelineConstants.SubmissionsType.PUBLIC;
        else
            this.submissionType = SubmissionPipelineConstants.SubmissionsType.valueOf(submissionType);
    }


    private void doProjectSync(String accession) {
        try {
            Project oracleProject = projectRepoClient.findByAccession(accession);
            if (!oracleProject.isPublicProject()) {
                return;
            }
            MongoPrideProject mongoPrideProject = PrideProjectTransformer.transformOracleToMongo(oracleProject);
            Optional<MongoPrideProject> status = prideProjectMongoService.upsert(mongoPrideProject);
            log.info(oracleProject.getAccession() + "-- project inserted Status " + status.isPresent());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private void doFileSync(String accession) {
        try {
            Optional<MongoPrideProject> mongoPrideProjectOptional = prideProjectMongoService.findByAccession(accession);
            if (!mongoPrideProjectOptional.isPresent()) {
                return;
            }
            MongoPrideProject mongoPrideProject = mongoPrideProjectOptional.get();
            Project oracleProject = projectRepoClient.findByAccession(mongoPrideProject.getAccession());
            List<ProjectFile> oracleFiles = fileRepoClient.findAllByProjectId(oracleProject.getId());

            List<MongoPrideMSRun> msRunRawFiles = new ArrayList<>();
            List<Tuple<MongoPrideFile, MongoPrideFile>> status = prideFileMongoService.insertAllFilesAndMsRuns(PrideProjectTransformer.transformOracleFilesToMongoFiles(oracleFiles, msRunRawFiles, oracleProject, ftpProtocol, asperaProtocol), msRunRawFiles);
            log.info("Number of files has been inserted -- " + status.size());
            if (msRunRawFiles.size() > 0) {
                //to-do
                log.info("Number of MS Run files has been inserted -- " + msRunRawFiles.size());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }


    /**
     * This methods connects to the database read all the Oracle information for public
     *
     * @return
     */
    @Bean
    Step syncProjectOracleToMongoDB() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC.name())
                .tasklet((stepContribution, chunkContext) -> {
                    setOverride(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("override"));
                    setSubmissionType(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("submissionType"));
                    //String accession = accession:chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("accession");
                    log.info("############# job param accession:" + accession);
                    if (accession != null) {
                        doProjectSync(accession);
                    } else {
                        projectRepoClient.getAllPublicAccessions().forEach(this::doProjectSync);
                    }
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
    public Step syncFileInformationToMongoDBStep() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC_FILES.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if (skipFiles != null && skipFiles) {
                        return RepeatStatus.FINISHED;
                    }
                    //String accession = chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("accession");
                    if (accession != null) {
                        doFileSync(accession);
                    } else {
                        prideProjectMongoService.getAllProjectAccessions().forEach(this::doFileSync);
                    }
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
                .next(syncFileInformationToMongoDBStep())
                .build();
    }

}
