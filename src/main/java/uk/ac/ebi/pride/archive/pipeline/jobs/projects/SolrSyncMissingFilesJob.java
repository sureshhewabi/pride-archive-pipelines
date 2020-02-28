package uk.ac.ebi.pride.archive.pipeline.jobs.projects;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, MoleculesMongoConfig.class, SolrCloudMasterConfig.class})
public class SolrSyncMissingFilesJob extends AbstractArchiveJob {

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    private SolrProjectService solrProjectService;

    private Map<String, Long> taskTimeMap = new HashMap<>();

    private String projectAccession;

    @Bean
    @StepScope
    public Tasklet initSolrSyncMissingFilesJob(@Value("#{jobParameters['project']}") String projectAccession) {
        return (stepContribution, chunkContext) ->
        {
            this.projectAccession = projectAccession;
            log.info(String.format("==================>>>>>>> initSolrSyncMissingFilesJob - Run the job for Project %s", projectAccession));
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Job solrSyncMissingFilesJobBean() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SOLR_SYNC_MISSING_FILES.getName())
                .start(stepBuilderFactory
                        .get("initSolrSyncMissingFilesJob")
                        .tasklet(initSolrSyncMissingFilesJob(null))
                        .build())
                .next(solrSyncMissingFilesStep())
                .next(solrSyncMissingFilesPrintTraceStep())
                .build();
    }

    @Bean
    public Step solrSyncMissingFilesPrintTraceStep() {
        return stepBuilderFactory
                .get("solrSyncMissingFilesPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step solrSyncMissingFilesStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SOLR_SYNC_MISSING_FILES.name())
                .tasklet((stepContribution, chunkContext) -> {

                    long initInsertPeptides = System.currentTimeMillis();

                    if (projectAccession == null) {
                        Set<String> projects = solrProjectService.findProjectAccessionsWithEmptyFileNames();
                        for (String accession : projects) {
                            List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(accession);
                            Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
                            updateSolrProject(accession, fileNames);
                            break;
                        }
                    } else {
                        List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(projectAccession);
                        Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
                        updateSolrProject(projectAccession, fileNames);
                    }

                    taskTimeMap.put("SolrSyncMissingFiles", System.currentTimeMillis() - initInsertPeptides);

                    return RepeatStatus.FINISHED;
                }).build();
    }

    private void updateSolrProject(String prjAccession, Set<String> fileNames) {
        PrideSolrProject solrProject = solrProjectService.findByAccession(prjAccession);
        if (solrProject == null) {
            return;
        }
        solrProject.setProjectFileNames(fileNames);
        solrProjectService.update(solrProject);
        log.info("updated solr project: " + prjAccession);
    }
}
