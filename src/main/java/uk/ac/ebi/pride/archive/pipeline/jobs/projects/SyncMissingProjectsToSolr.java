package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

import java.util.*;
import java.util.stream.Collectors;


@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, MoleculesMongoConfig.class, SolrCloudMasterConfig.class})
public class SyncMissingProjectsToSolr extends AbstractArchiveJob {

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    @Autowired
    private SolrProjectService solrProjectService;

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    private PrideMoleculesMongoService prideMoleculesMongoService;

    private Map<String, Long> taskTimeMap = new HashMap<>();

    @Bean
    public Job syncMissingProjectsToSolrJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SYNC_MISSING_PROJECTS_SOLR.getName())
                .start(solrSyncMissingProjectsStep())
                .next(syncMissingProjectsToSolrPrintTraceStep())
                .build();
    }

    @Bean
    public Step syncMissingProjectsToSolrPrintTraceStep() {
        return stepBuilderFactory
                .get("syncMissingProjectsToSolrPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step solrSyncMissingProjectsStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SYNC_MISSING_PROJECTS_SOLR.name())
                .tasklet((stepContribution, chunkContext) -> {

                    long initTime = System.currentTimeMillis();

                    final Set<String> solrProjectAccessions = getSolrProjectAccessions();
                    final Set<String> mongoProjectAccessions = getMongoProjectAccessions();

                    Set<String> onlyInSolr = new HashSet<>(solrProjectAccessions);
                    onlyInSolr.removeAll(mongoProjectAccessions);

                    Set<String> onlyInMongo = new HashSet<>(mongoProjectAccessions);
                    onlyInMongo.removeAll(solrProjectAccessions);

                    doProjectSync(onlyInMongo);
                    removeExtraProjects(onlyInSolr);

                    taskTimeMap.put("SyncMissingProjectsToSolr", System.currentTimeMillis() - initTime);

                    return RepeatStatus.FINISHED;
                }).build();
    }

    private void doProjectSync(Set<String> accessions) {
        accessions.forEach(i -> {
            Optional<MongoPrideProject> mongoPrideProjectOptional = prideProjectMongoService.findByAccession(i);
            if (mongoPrideProjectOptional.isPresent()) {
                MongoPrideProject mongoPrideProject = mongoPrideProjectOptional.get();
                PrideSolrProject solrProject = PrideProjectTransformer.transformProjectMongoToSolr(mongoPrideProject);

                List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(i);
                Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
                solrProject.setProjectFileNames(fileNames);

//                Set<String> proteinAccessions = new HashSet<>(prideMoleculesMongoService.findProteinAccessionByProjectAccessions(i));
//                Set<String> peptideSequences = new HashSet<>(prideMoleculesMongoService.findPeptideSequenceByProjectAccessions(i));
//
//                solrProject.addProteinIdentifications(proteinAccessions);
//                solrProject.addPeptideSequences(peptideSequences);

                PrideSolrProject status = solrProjectService.save(solrProject);
                log.info("The project -- " + status.getAccession() + " has been inserted in SolrCloud");
            }
        });
    }

    private void removeExtraProjects(Set<String> accessions) {
        accessions.forEach(i -> {
            PrideSolrProject prideSolrProject = solrProjectService.findByAccession(i);
            if (prideSolrProject != null) {
                String id = (String) prideSolrProject.getId();
                solrProjectService.deleteProjectById(id);
                log.info("Document with id-accession: " + id + " - " + prideSolrProject.getAccession() + " has been deleted from the SolrCloud Master");
            }
        });
    }

    private Set<String> getMongoProjectAccessions() {
        Set<String> mongoProjectAccessions = prideProjectMongoService.getAllProjectAccessions();
        log.info("Number of MongoDB projects: " + mongoProjectAccessions.size());
        return mongoProjectAccessions;
    }

    private Set<String> getSolrProjectAccessions() {
        Set<String> solrProjectAccessions = new HashSet<>();
        final Iterable<PrideSolrProject> solrProjects = solrProjectService.findAll();
        solrProjects.forEach(x -> {
            solrProjectAccessions.add(x.getAccession());
        });
        log.info("Number of Solr Projects: " + solrProjectAccessions.size());
        return solrProjectAccessions;
    }
}
