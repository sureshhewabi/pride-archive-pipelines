package uk.ac.ebi.pride.archive.pipeline.jobs.molecules;


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
import uk.ac.ebi.pride.archive.pipeline.utility.BackupUtil;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, MoleculesMongoConfig.class, SolrCloudMasterConfig.class})
public class SolrIndexProteinPeptideJob extends AbstractArchiveJob {

    @Autowired
    private SolrProjectService solrProjectService;

    private Map<String, Long> taskTimeMap = new HashMap<>();

    @Value("${project:#{null}}")
    private String projectAccession;

    @Value("${pride.data.backup.path}")
    String backupPath;

    @Bean
    @StepScope
    public Tasklet initJobSolrIndexProteinPeptideJob() {
        return (stepContribution, chunkContext) ->
        {
            log.info(String.format("==================>>>>>>> SolrIndexProteinPeptideJob - Run the job for Project %s", projectAccession));
            backupPath = backupPath.endsWith(File.separator) ? backupPath : backupPath + File.separator;
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Job solrIndexPeptideProteinJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SOLR_INDEX_PEPTIDE_PROTEIN.getName())
                .start(stepBuilderFactory
                        .get("initJobSolrIndexProteinPeptideJob")
                        .tasklet(initJobSolrIndexProteinPeptideJob())
                        .build())
                .next(solrIndexProteinPeptideIndexStep())
                .next(solrIndexPrintTraceStep())
                .build();
    }

    @Bean
    public Step solrIndexPrintTraceStep() {
        return stepBuilderFactory
                .get("solrIndexPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step solrIndexProteinPeptideIndexStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SOLR_INDEX_PEPTIDE_PROTEIN.name())
                .tasklet((stepContribution, chunkContext) -> {

                    long initInsertPeptides = System.currentTimeMillis();

                    if (projectAccession == null) {
                        for (Path d : Files.newDirectoryStream(Paths.get(backupPath), path -> path.toFile().isDirectory())) {
                            restoreFromFile(d.getFileName().toString());
                        }
                    } else {
                        restoreFromFile(projectAccession);
                    }

                    taskTimeMap.put("InsertPeptidesProteinsIntoSolr", System.currentTimeMillis() - initInsertPeptides);

                    return RepeatStatus.FINISHED;
                }).build();
    }

    private void restoreFromFile(String projectAccession) throws Exception {
        PrideSolrProject solrProject = solrProjectService.findByAccession(projectAccession);
        if (solrProject == null) {
            return;
        }
        String dir = backupPath + projectAccession;
        Set<String> proteinAccessions = new HashSet<>();
        Set<String> peptideSequences = new HashSet<>();
        for (Path f : Files.newDirectoryStream(Paths.get(dir), path -> path.toFile().isFile())) {
            if (f.getFileName().toString().endsWith(PrideMongoPeptideEvidence.class.getSimpleName() + BackupUtil.JSON_EXT)) {
                List<PrideMongoPeptideEvidence> objs = BackupUtil.getObjectsFromFile(f, PrideMongoPeptideEvidence.class);
                objs.forEach(o -> {
                    proteinAccessions.add(o.getProteinAccession());
                    peptideSequences.add(o.getPeptideSequence());
                });
            }
        }

        solrProject.addProteinIdentifications(proteinAccessions);
        solrProject.addPeptideSequences(peptideSequences);
        solrProjectService.update(solrProject);
        log.info("updated solr project: " + projectAccession);
    }
}
