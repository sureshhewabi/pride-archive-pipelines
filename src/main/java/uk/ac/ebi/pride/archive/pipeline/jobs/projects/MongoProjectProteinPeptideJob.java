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
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.BackupUtil;
import uk.ac.ebi.pride.mongodb.archive.model.molecules.MongoPrideMolecules;
import uk.ac.ebi.pride.mongodb.archive.service.molecules.PrideMoleculesMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, PrideMoleculesMongoService.class})
public class MongoProjectProteinPeptideJob extends AbstractArchiveJob {

    private Map<String, Long> taskTimeMap = new HashMap<>();

    @Value("${project:#{null}}")
    private String projectAccession;

    @Value("${pride.data.backup.path}")
    String backupPath;

    @Autowired
    private PrideMoleculesMongoService prideMoleculesMongoService;

    @Bean
    @StepScope
    public Tasklet initMongoProjectProteinPeptideJob() {
        return (stepContribution, chunkContext) ->
        {
            log.info(String.format("==================>>>>>>> MongoProjectProteinPeptideJob - Run the job for Project %s", projectAccession));
            backupPath = backupPath.endsWith(File.separator) ? backupPath : backupPath + File.separator;
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Job mongoProjectProteinPeptideJobBean() {
        return jobBuilderFactory
                .get("mongoProjectProteinPeptideJobBean")
                .start(stepBuilderFactory
                        .get("initMongoProjectProteinPeptideJob")
                        .tasklet(initMongoProjectProteinPeptideJob())
                        .build())
                .next(mongoProjectProteinPeptideStep())
                .next(mongoProjectProteinPeptideJobPrintTraceStep())
                .build();
    }

    @Bean
    public Step mongoProjectProteinPeptideJobPrintTraceStep() {
        return stepBuilderFactory
                .get("mongoProjectProteinPeptideJobPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step mongoProjectProteinPeptideStep() {
        return stepBuilderFactory
                .get("mongoProjectProteinPeptideStep")
                .tasklet((stepContribution, chunkContext) -> {

                    long initInsertPeptides = System.currentTimeMillis();

                    if (projectAccession == null) {
                        for (Path d : Files.newDirectoryStream(Paths.get(backupPath), path -> path.toFile().isDirectory())) {
                            restoreFromFile(d.getFileName().toString());
                        }
                    } else {
                        restoreFromFile(projectAccession);
                    }

                    taskTimeMap.put("mongoProjectProteinPeptide", System.currentTimeMillis() - initInsertPeptides);

                    return RepeatStatus.FINISHED;
                }).build();
    }

    private void restoreFromFile(String projectAccession) throws Exception {
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

        MongoPrideMolecules mongoPrideMolecules = MongoPrideMolecules.builder().projectAccession(projectAccession)
                .peptideAccessions(peptideSequences).proteinAccessions(proteinAccessions).build();

        prideMoleculesMongoService.insert(mongoPrideMolecules);
    }
}
