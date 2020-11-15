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
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.BackupUtil;
import uk.ac.ebi.pride.mongodb.archive.service.molecules.PrideProjectMoleculesMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.model.psm.PrideMongoPsmSummaryEvidence;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, PrideProjectMoleculesMongoService.class})
public class MongoUpdatePsmJob extends AbstractArchiveJob {

    private Map<String, Long> taskTimeMap = new HashMap<>();

    @Autowired
    private PrideMoleculesMongoService prideMoleculesMongoService;
    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Value("${accession:#{null}}")
    @StepScope
    private String accession;

    @Value("${pride.data.backup.path}")
    String assaysBackupPath;

    @Bean
    @StepScope
    public Tasklet initMongoUpdatePsmJob() {
        return (stepContribution, chunkContext) ->
        {
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Job mongoUpdatePsmJobJobBean() {
        return jobBuilderFactory
                .get("mongoUpdatePsmJobJobBean")
                .start(stepBuilderFactory
                        .get("initMongoUpdatePsmJob")
                        .tasklet(initMongoUpdatePsmJob())
                        .build())
                .next(mongoUpdatePsmStep())
                .next(mongoUpdatePsmJobPrintTraceStep())
                .build();
    }

    @Bean
    public Step mongoUpdatePsmJobPrintTraceStep() {
        return stepBuilderFactory
                .get("mongoUpdatePsmJobPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step mongoUpdatePsmStep() {
        return stepBuilderFactory
                .get("mongoUpdatePsmStep")
                .tasklet((stepContribution, chunkContext) -> {
                    long start = System.currentTimeMillis();
                    if (accession != null) {
                        log.info("Project : " + accession);
//                        prideMoleculesMongoService.addSpectraUsi(accession);
                        addSpectraUsiToBackupFilesAndMongo(accession);
                    } else {
                        Set<String> allProjectAccessions = prideProjectMongoService.getAllProjectAccessions();
                        allProjectAccessions.forEach(p -> {
                            try {
                                log.info("Project : " + p);
//                                prideMoleculesMongoService.addSpectraUsi(p);
                                addSpectraUsiToBackupFilesAndMongo(p);
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                            }
                        });
                    }

                    taskTimeMap.put("mongoUpdatePsmStep", System.currentTimeMillis() - start);
                    return RepeatStatus.FINISHED;
                }).build();
    }

    private void addSpectraUsiToBackupFilesAndMongo(String projectAccession) throws Exception {
        if (!assaysBackupPath.endsWith(File.separator)) {
            assaysBackupPath = assaysBackupPath + File.separator;
        }
        String pathname = assaysBackupPath + projectAccession;
        File assaysDir = new File(pathname);
        FilenameFilter filter = (dir, name) -> name.endsWith(PrideMongoPsmSummaryEvidence.class.getSimpleName() + ".json");
        String[] psmSummaryEvidenceFiles = assaysDir.list(filter);
        for (String s2 : psmSummaryEvidenceFiles) {
            String prjAssayAcc = s2.substring(0, org.apache.commons.lang3.StringUtils.ordinalIndexOf(s2, "_", 2));
            String filePath = pathname + File.separator + s2;
            Map<String, String> map = new HashMap<>();
            List<PrideMongoPsmSummaryEvidence> objs = BackupUtil.getObjectsFromFile(Paths.get(filePath), PrideMongoPsmSummaryEvidence.class);
            for (PrideMongoPsmSummaryEvidence obj : objs) {
                String usi = obj.getUsi();
                String spectraUsi = usi.substring(0, org.apache.commons.lang3.StringUtils.ordinalIndexOf(usi, ":", 5));
                if (obj.getSpectraUsi() == null || !obj.getSpectraUsi().equals(spectraUsi)) {
                    obj.setSpectraUsi(spectraUsi);
                    map.put(usi, spectraUsi);
                }
            }
            if (!map.isEmpty()) {
                BufferedWriter psmSummaryEvidenceBufferedWriter = new BufferedWriter(new FileWriter(filePath, false));
                for (PrideMongoPsmSummaryEvidence obj : objs) {
                    BackupUtil.write(obj, psmSummaryEvidenceBufferedWriter);
                }
                log.info("[" + prjAssayAcc + "] Updated Json file :" + filePath);
                psmSummaryEvidenceBufferedWriter.close();
                long l = prideMoleculesMongoService.addSpectraUsi(map);
                log.info("[" + prjAssayAcc + "] Updated PSMs count :" + l);
            }
        }
    }
}
