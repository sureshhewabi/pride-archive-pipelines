package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
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
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.msrun.MongoPrideMSRun;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.utilities.util.Tuple;

import java.util.ArrayList;
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

    @Value("${accession:#{null}}")
    @StepScope
    private String accession;


    /**
     * This method take the parameter from the override. If this value is True then the MongoDB
     * data will be overrid with the data from the Oracle Database.
     *
     * @param override
     */
    public void setOverride(final String override){
        this.override = override != null && override.equalsIgnoreCase("TRUE");
    }

    /**
     * The submission type enables to know which data will be submitted to MongoDB
     * @param submissionType The type of Submissions will be push into MongoDb {@link uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants.SubmissionsType}
     */
    public void setSubmissionType (final String submissionType){
        if(submissionType == null)
            this.submissionType = SubmissionPipelineConstants.SubmissionsType.PUBLIC;
        else
            this.submissionType = SubmissionPipelineConstants.SubmissionsType.valueOf(submissionType);
    }


    private void doProjectSync(Project oracleProject){
        if(!(!oracleProject.isPublicProject() && (submissionType == SubmissionPipelineConstants.SubmissionsType.PUBLIC))) {
            MongoPrideProject mongoPrideProject = PrideProjectTransformer.transformOracleToMongo(oracleProject);
            Optional<MongoPrideProject> status = prideProjectMongoService.insert(mongoPrideProject);
            log.info(oracleProject.getAccession() + "-- Inserted Status " + String.valueOf(status.isPresent()));
        }
    }

    private  void doFileSync(MongoPrideProject mongoPrideProject){
        Project oracleProject = oracleProjectRepository.findByAccession(mongoPrideProject.getAccession());
        List<ProjectFile> oracleFiles = oracleFileRepository.findAllByProjectId(oracleProject.getId());
        List<MongoPrideMSRun> msRunRawFiles = new ArrayList<>();
        int accessionSequence = prideFileMongoService.getNextAccessionNumber(oracleFiles.size());
        List<Tuple<MongoPrideFile, MongoPrideFile>> status = prideFileMongoService.insertAllFilesAndMsRuns(PrideProjectTransformer.transformOracleFilesToMongoFiles(oracleFiles,msRunRawFiles, oracleProject, ftpProtocol, asperaProtocol,accessionSequence),msRunRawFiles);
        log.info("The following files has been inserted -- " + status.toString());
        if(msRunRawFiles.size()>0){
            //to-do
            log.info("The following MS Run files has been inserted -- " + status.toString());
        }
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
                    //String accession = accession:chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("accession");
                    System.out.println("############# job param accession:"+accession);
                    if(accession != null){
                        Project oracleProject = oracleProjectRepository.findByAccession(accession);
                        doProjectSync(oracleProject);
                    }else {
                        oracleProjectRepository.findAll().forEach(this::doProjectSync);
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
    public Step syncFileInformationToMongoDB() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC_FILES.name())
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("############# job param accession:"+accession);
                    //String accession = chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("accession");
                    if(accession != null){
                        Optional<MongoPrideProject> mongoPrideProjectOptional = prideProjectMongoService.findByAccession(accession);
                        if(mongoPrideProjectOptional.isPresent()) {
                            MongoPrideProject mongoPrideProject = mongoPrideProjectOptional.get();
                            doFileSync(mongoPrideProject);
                        }
                    }else {
                        prideProjectMongoService.findAllStream().forEach(this::doFileSync);
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
                .next(syncFileInformationToMongoDB())
                .build();
    }

}
