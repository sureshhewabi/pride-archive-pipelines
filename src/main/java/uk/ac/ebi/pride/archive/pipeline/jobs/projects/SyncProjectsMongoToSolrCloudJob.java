package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.solr.api.client.SolrProjectClient;
import uk.ac.ebi.pride.solr.commons.PrideSolrProject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SyncProjectsMongoToSolrCloudJob extends AbstractArchiveJob {

    protected PrideProjectMongoService prideProjectMongoService;

    protected PrideFileMongoService prideFileMongoService;

    protected SolrProjectClient solrProjectClient;

    @Value("${accession:#{null}}")
    protected String accession;

    public SyncProjectsMongoToSolrCloudJob(PrideProjectMongoService prideProjectMongoService,
                                           PrideFileMongoService prideFileMongoService,
                                           SolrProjectClient solrProjectClient) {
        this.prideProjectMongoService = prideProjectMongoService;
        this.prideFileMongoService = prideFileMongoService;
        this.solrProjectClient = solrProjectClient;
    }

    protected Tasklet syncProjectMongoToSolrTasklet() {
        return (stepContribution, chunkContext) -> {
            if (accession != null) {
                Optional<MongoPrideProject> mongoPrideProjectOptional = prideProjectMongoService.findByAccession(accession);
                if (mongoPrideProjectOptional.isPresent()) {
                    MongoPrideProject mongoPrideProject = mongoPrideProjectOptional.get();
                    doProjectSync(mongoPrideProject);
                }
            } else {
                int page = 0;
                int size = 1000;
                while (true) {
                    List<MongoPrideProject> mongoPrideProjects =
                            prideProjectMongoService.findAll(PageRequest.of(page++, size)).getContent();
                    if (mongoPrideProjects != null && mongoPrideProjects.size() >= 1) {
                        List<PrideSolrProject> prideSolrProjects = mongoPrideProjects.stream()
                                .map(mongoPrideProject -> getPrideSolrProjectFromMongoProject(mongoPrideProject))
                                .collect(Collectors.toList());
                        saveBulkSolrProjects(prideSolrProjects);
                        log.info((page - 1) * 1000 + mongoPrideProjects.size() + " projects has been inserted");
                    } else {
                        break;
                    }
                }
            }
            return RepeatStatus.FINISHED;
        };
    }

    private void doProjectSync(MongoPrideProject mongoPrideProject) {
        PrideSolrProject solrProject = getPrideSolrProjectFromMongoProject(mongoPrideProject);
        PrideSolrProject status = null;
        try {
            status = solrProjectClient.save(solrProject);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
        log.info("The project -- " + status.getAccession() + " has been inserted in SolrCloud");
    }

//    private void doFilesSync(PrideSolrProject prideSolrProject){
//        List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(prideSolrProject.getAccession());
//        Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
//        prideSolrProject.setProjectFileNames(fileNames);
//        PrideSolrProject savedProject = solrProjectService.update(prideSolrProject);
//        log.info("The files for project -- " + savedProject.getAccession() + " have been inserted in SolrCloud");
//    }

    private void saveBulkSolrProjects(List<PrideSolrProject> prideSolrProjects) {
        try {
            solrProjectClient.saveAll(prideSolrProjects);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private PrideSolrProject getPrideSolrProjectFromMongoProject(MongoPrideProject mongoPrideProject) {
        PrideSolrProject solrProject = PrideProjectTransformer.transformProjectMongoToSolr(mongoPrideProject);
        List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(mongoPrideProject.getAccession());
        Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
        solrProject.setProjectFileNames(fileNames);
        return solrProject;
    }


    protected Tasklet cleanSolrCloudTasklet() {
        return (stepContribution, chunkContext) -> {
            System.out.println("#####################Accession:" + accession);
            if (accession != null) {
                Optional<PrideSolrProject> prideSolrProject = solrProjectClient.findByAccession(accession);
                if (prideSolrProject.isPresent()) {
                    String id = (String) prideSolrProject.get().getId();
                    solrProjectClient.deleteProjectById(id);
                    log.info("Document with id-accession: " + id + " - " + prideSolrProject.get().getAccession() + " has been deleted from the SolrCloud Master");
                }
            } else {
                try {
                    solrProjectClient.deleteAll();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    throw new IllegalStateException(e);
                }
                log.info("All Documents has been deleted from the SolrCloud Master");
            }
            return RepeatStatus.FINISHED;
        };
    }

    /**
     * Sync the Files to Solr Project
     * @return Step
     */
//    @Bean
//    Step syncFilesToSolrProjectStep() {
//        return stepBuilderFactory
//                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SYNC_FILES_TO_PROJECT_SOLR.name())
//                .tasklet((stepContribution, chunkContext) -> {
//                    if(accession != null){
//                        PrideSolrProject prideSolrProject = solrProjectService.findByAccession(accession);
//                        doFilesSync(prideSolrProject);
//                    }else{
//                        solrProjectService.findAll().forEach(this::doFilesSync);
//                    }
//                    return RepeatStatus.FINISHED;
//                }).build();
//    }


}
