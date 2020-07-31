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
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
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
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


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
@Import({RepoConfig.class, ArchiveMongoConfig.class, DataSourceConfiguration.class, SolrCloudMasterConfig.class})
public class SyncProjectsToMongoAndSolrJob extends AbstractArchiveJob {

    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    SolrProjectService solrProjectService;

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

    private void doProjectSync(String accession) {
        try {
            Project oracleProject = projectRepoClient.findByAccession(accession);
            if (!oracleProject.isPublicProject()) {
                return;
            }
            MongoPrideProject mongoPrideProject = PrideProjectTransformer.transformOracleToMongo(oracleProject);
            Optional<MongoPrideProject> status = prideProjectMongoService.upsert(mongoPrideProject);
            log.info(oracleProject.getAccession() + "-- [Mongo] project inserted Status " + status.isPresent());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private void doMongoFileSync(String accession) {
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
            log.info("[Mongo] Number of files has been inserted -- " + status.size());
            if (msRunRawFiles.size() > 0) {
                //to-do
                log.info("[Mongo] Number of MS Run files has been inserted -- " + msRunRawFiles.size());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    @Bean
    Step syncProjectToMongoDB() {
        return stepBuilderFactory
                .get("syncProjectToMongoDB")
                .tasklet((stepContribution, chunkContext) -> {
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

    @Bean
    public Step syncProjectFilesToMongoDB() {
        return stepBuilderFactory.get("syncProjectFilesToMongoDB")
                .tasklet((stepContribution, chunkContext) -> {
                    if (skipFiles != null && skipFiles) {
                        return RepeatStatus.FINISHED;
                    }
                    if (accession != null) {
                        doMongoFileSync(accession);
                    } else {
                        prideProjectMongoService.getAllProjectAccessions().forEach(this::doMongoFileSync);
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    private void doSolrSync(MongoPrideProject mongoPrideProject){
        PrideSolrProject solrProject = PrideProjectTransformer.transformProjectMongoToSolr(mongoPrideProject);
        List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(mongoPrideProject.getAccession());
        Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
        solrProject.setProjectFileNames(fileNames);
        PrideSolrProject status = solrProjectService.upsert(solrProject);
        log.info("[Solr] The project -- " + status.getAccession() + " has been inserted in SolrCloud");
    }

    @Bean
    Step syncMongoProjectToSolrStep() {
        return stepBuilderFactory
                .get("syncMongoProjectToSolrStep")
                .tasklet((stepContribution, chunkContext) -> {
                    if(accession != null){
                        Optional<MongoPrideProject> mongoPrideProjectOptional = prideProjectMongoService.findByAccession(accession);
                        if(mongoPrideProjectOptional.isPresent()) {
                            MongoPrideProject mongoPrideProject = mongoPrideProjectOptional.get();
                            doSolrSync(mongoPrideProject);
                        }
                    }else{
                        prideProjectMongoService.findAllStream().forEach(this::doSolrSync);
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Job syncProjectToMongoAndSolrJob() {
        return jobBuilderFactory
                .get("syncProjectToMongoAndSolrJob")
                .start(syncProjectToMongoDB())
                .next(syncProjectFilesToMongoDB())
                .next(syncMongoProjectToSolrStep())
                .build();
    }

}
