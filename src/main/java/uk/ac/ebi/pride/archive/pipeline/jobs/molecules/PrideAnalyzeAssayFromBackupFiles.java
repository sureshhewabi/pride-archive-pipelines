package uk.ac.ebi.pride.archive.pipeline.jobs.molecules;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.BackupUtil;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.spectra.configs.AWS3Configuration;
import uk.ac.ebi.pride.archive.spectra.model.ArchiveSpectrum;
import uk.ac.ebi.pride.archive.spectra.services.S3SpectralArchive;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;
import uk.ac.ebi.pride.mongodb.molecules.model.protein.PrideMongoProteinEvidence;
import uk.ac.ebi.pride.mongodb.molecules.model.psm.PrideMongoPsmSummaryEvidence;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, MoleculesMongoConfig.class, AWS3Configuration.class})
public class PrideAnalyzeAssayFromBackupFiles extends AbstractArchiveJob {
    @Autowired
    PrideMoleculesMongoService moleculesService;

    @Autowired
    S3SpectralArchive spectralArchive;

    @Value("${pride.data.prod.directory}")
    String productionPath;

    @Value("${pride.data.backup.path}")
    String backupPath;

    private Map<String, Long> taskTimeMap = new HashMap<>();

    private String projectAccession;
    private String assayAccession;

    @Bean
    @StepScope
    public Tasklet initPrideAnalyzeAssayFromBackupFilesJob(@Value("#{jobParameters['project']}") String projectAccession,
                                                           @Value("#{jobParameters['assay']}") String assayAccession) {
        return (stepContribution, chunkContext) ->
        {
            this.projectAccession = projectAccession;
            this.assayAccession = assayAccession;
            System.out.println(String.format("==================>>>>>>> SolrIndexProteinPeptideJob - Run the job for Project %s", projectAccession));
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Job prideAnalyzeAssayFromBackupFilesJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ANALYZE_ASSAY_FROM_BACKUP_FILES.getName())
                .start(stepBuilderFactory
                        .get("initPrideAnalyzeAssayFromBackupFilesJob")
                        .tasklet(initPrideAnalyzeAssayFromBackupFilesJob(null, null))
                        .build())
                .next(prideAnalyzeAssayFromBackupFilesStep())
                .next(prideAnalyzeAssayFromBackupFilesPrintTraceStep())
                .build();
    }

    @Bean
    public Step prideAnalyzeAssayFromBackupFilesPrintTraceStep() {
        return stepBuilderFactory
                .get("prideAnalyzeAssayFromBackupFilesPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step prideAnalyzeAssayFromBackupFilesStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SOLR_INDEX_PEPTIDE_PROTEIN_FROM_FILE.name())
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

                        long tm = System.currentTimeMillis();

                        if (projectAccession == null) {
                            for (Path d : Files.newDirectoryStream(Paths.get(backupPath), path -> path.toFile().isDirectory())) {
                                for (Path f : Files.newDirectoryStream(d, path -> path.toFile().isFile())) {
                                    restoreFromFile(f);
                                }
                            }
                        } else if (assayAccession == null) {
                            String dir = backupPath.endsWith(File.separator) ? backupPath + projectAccession : backupPath + File.separator + projectAccession;
                            for (Path f : Files.newDirectoryStream(Paths.get(dir), path -> path.toFile().isFile())) {
                                restoreFromFile(f);
                            }
                        } else {
                            restoreFromFiles(projectAccession, assayAccession);
                        }

                        taskTimeMap.put("PrideAnalyzeAssayFromBackupFiles", System.currentTimeMillis() - tm);

                        return RepeatStatus.FINISHED;
                    }
                }).build();
    }

    private void restoreFromFile(Path file) throws Exception {
        String fileName = file.getFileName().toString();
        if (fileName.endsWith(PrideMongoPsmSummaryEvidence.class.getSimpleName() + BackupUtil.JSON_EXT)) {
            List<PrideMongoPsmSummaryEvidence> objs = BackupUtil.getObjectsFromFile(file, PrideMongoPsmSummaryEvidence.class);
            objs.forEach(o -> moleculesService.savePsmSummaryEvidence(o));
        } else if (fileName.endsWith(PrideMongoPeptideEvidence.class.getSimpleName() + BackupUtil.JSON_EXT)) {
            List<PrideMongoPeptideEvidence> objs = BackupUtil.getObjectsFromFile(file, PrideMongoPeptideEvidence.class);
            objs.forEach(o -> moleculesService.savePeptideEvidence(o));
        } else if (fileName.endsWith(PrideMongoProteinEvidence.class.getSimpleName() + BackupUtil.JSON_EXT)) {
            List<PrideMongoProteinEvidence> objs = BackupUtil.getObjectsFromFile(file, PrideMongoProteinEvidence.class);
            objs.forEach(o -> moleculesService.saveProteinEvidences(o));
        } else if (fileName.endsWith(ArchiveSpectrum.class.getSimpleName() + BackupUtil.JSON_EXT)) {
            List<ArchiveSpectrum> objs = BackupUtil.getObjectsFromFile(file, ArchiveSpectrum.class);
            for (ArchiveSpectrum o : objs) {
                spectralArchive.writePSM(o.getUsi(), o);
            }
        }
    }

    private void restoreFromFiles(String projectAccession, String assayAccession) throws Exception {
        List<PrideMongoProteinEvidence> prideMongoProteinEvidences = BackupUtil.getPrideMongoProteinEvidenceFromBackup(backupPath, projectAccession, assayAccession);
        prideMongoProteinEvidences.forEach(p -> moleculesService.saveProteinEvidences(p));

        List<PrideMongoPeptideEvidence> prideMongoPeptideEvidences = BackupUtil.getPrideMongoPeptideEvidenceFromBackup(backupPath, projectAccession, assayAccession);
        prideMongoPeptideEvidences.forEach(p -> moleculesService.savePeptideEvidence(p));

        List<PrideMongoPsmSummaryEvidence> prideMongoPsmSummaryEvidences = BackupUtil.getPrideMongoPsmSummaryEvidenceFromBackup(backupPath, projectAccession, assayAccession);
        prideMongoPsmSummaryEvidences.forEach(p -> moleculesService.savePsmSummaryEvidence(p));

        List<ArchiveSpectrum> archiveSpectrums = BackupUtil.getArchiveSpectrumFromBackup(backupPath, projectAccession, assayAccession);
        for (ArchiveSpectrum p : archiveSpectrums) {
            spectralArchive.writePSM(p.getUsi(), p);
        }
    }
}
