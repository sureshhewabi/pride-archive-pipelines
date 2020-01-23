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
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, MoleculesMongoConfig.class, SolrCloudMasterConfig.class})
public class SolrIndexProteinPeptideJob extends AbstractArchiveJob {

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    @Autowired
    private PrideMoleculesMongoService prideMoleculesMongoService;

    @Autowired
    private SolrProjectService solrProjectService;

    private Map<String, Long> taskTimeMap = new HashMap<>();

    private String projectAccession;

    @Bean
    @StepScope
    public Tasklet initJobSolrIndexProteinPeptideJob(@Value("#{jobParameters['project']}") String projectAccession) {
        return (stepContribution, chunkContext) ->
        {
            this.projectAccession = projectAccession;
            System.out.println(String.format("==================>>>>>>> SolrIndexProteinPeptideJob - Run the job for Project %s", projectAccession));
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Job solrIndexPeptideProteinJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SOLR_INDEX_PEPTIDE_PROTEIN.getName())
                .start(stepBuilderFactory
                        .get("initJobSolrIndexProteinPeptideJob")
                        .tasklet(initJobSolrIndexProteinPeptideJob(null))
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
                        Set<String> projects = solrProjectService.findProjectAccessionsWithEmptyPeptideSequencesOrProteinIdentifications();
                        for (String accession : projects) {
                            Set<String> proteinAccessions = new HashSet<>(prideMoleculesMongoService
                                    .findProteinAccessionByProjectAccessions(accession));
                            Set<String> peptideSequences = new HashSet<>(prideMoleculesMongoService
                                    .findPeptideSequenceByProjectAccessions(accession));
                            updateSolrProject(accession, proteinAccessions, peptideSequences);
                        }
                    } else {
                        Set<String> proteinAccessions = new HashSet<>(prideMoleculesMongoService
                                .findProteinAccessionByProjectAccessions(projectAccession));
                        Set<String> peptideSequences = new HashSet<>(prideMoleculesMongoService
                                .findPeptideSequenceByProjectAccessions(projectAccession));
                        updateSolrProject(projectAccession, proteinAccessions, peptideSequences);
                    }

                    taskTimeMap.put("InsertPeptidesProteinsIntoSolr", System.currentTimeMillis() - initInsertPeptides);

                    return RepeatStatus.FINISHED;
                }).build();
    }

    private void updateSolrProject(String prjAccession, Set<String> proteinIds, Set<String> peptideSequences) {
        PrideSolrProject solrProject = solrProjectService.findByAccession(prjAccession);
        if (solrProject == null) {
            return;
        }
        solrProject.addProteinIdentifications(proteinIds);
        solrProject.addPeptideSequences(peptideSequences);
        solrProjectService.update(solrProject);
    }
}
