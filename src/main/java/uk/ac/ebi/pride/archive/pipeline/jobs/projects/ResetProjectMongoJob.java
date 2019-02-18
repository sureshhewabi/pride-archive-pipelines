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
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.repos.file.ProjectFileRepository;
import uk.ac.ebi.pride.archive.repo.repos.project.ProjectRepository;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;

import java.util.List;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveOracleConfig.class, ArchiveMongoConfig.class, DataSourceConfiguration.class})
public class ResetProjectMongoJob extends AbstractArchiveJob {
    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    ProjectFileRepository oracleFileRepository;

    @Autowired
    ProjectRepository oracleProjectRepository;

    @Value("${accession:#{null}}")
    @StepScope
    private String accession;

    /**
     * This methods resets the data of a project from Mongo DB
     * @return
     */
    @Bean
    Step resetProjectMongoDB() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_RESET_SUBMISSION_MONGO.name())
                .tasklet((stepContribution, chunkContext) -> {
                    //String accession = accession:chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters().getString("accession");
                    System.out.println("############# job param accession:"+accession);
                    boolean deleteSuccess = prideProjectMongoService.deleteByAccession(accession);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * This methods resets the files data of a project from Mongo DB
     * The files can be shared across projects in future.
     * Hence, check if files are being referred by other projects before deleting them
     * @return Step
     */
    @Bean
    public Step resetFileInformationMongoDB() {
        return stepBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC_FILES.name())
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("############# job param accession:"+accession);
                    if(isSubmissionResetSafe()){
                        prideFileMongoService.deleteByAccession(accession);
                    }else{
                        throw new Exception("Files cannot be safely removed for selected project accession. Please check if other projects are refering to these files.");
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();


    }

    private boolean isSubmissionResetSafe() throws Exception{
        if(accession!=null){
            //check for files in collection
            List<MongoPrideFile> prideFilesList = prideFileMongoService.findFilesByProjectAccession(accession);
            for(MongoPrideFile prideFile : prideFilesList){
                if(prideFile.getProjectAccessions().size()>1){
                    //file accessed by multiple projects, cannot be reset/deleted
                    return false;
                }
            }
            return true;
        }else{
            throw new NullPointerException("Accession cannot be null");
        }
    }

    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job syncOracleToMongoProjectsJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_RESET_SUBMISSION_MONGODB.getName())
                .start(resetFileInformationMongoDB())
                .on("COMPLETED").to(resetProjectMongoDB())
                .end()
                .build();
    }
}
