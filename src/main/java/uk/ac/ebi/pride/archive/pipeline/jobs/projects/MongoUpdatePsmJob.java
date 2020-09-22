package uk.ac.ebi.pride.archive.pipeline.jobs.projects;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.mongodb.archive.service.molecules.PrideProjectMoleculesMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.model.psm.PrideMongoPsmSummaryEvidence;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;

import java.util.*;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, PrideProjectMoleculesMongoService.class})
public class MongoUpdatePsmJob extends AbstractArchiveJob {

    private Map<String, Long> taskTimeMap = new HashMap<>();

    @Autowired
    private PrideMoleculesMongoService prideMoleculesMongoService;

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

                    int i=0;
                    while (true) {
                        Page<PrideMongoPsmSummaryEvidence> prideMongoPsmSummaryEvidences = prideMoleculesMongoService.listPsmSummaryEvidences(PageRequest.of(i++, 100));
                        List<PrideMongoPsmSummaryEvidence> psms = prideMongoPsmSummaryEvidences.getContent();
                        if(psms.isEmpty()) {
                            break;
                        }
                        log.info("Page number : "+ i);
                        psms.parallelStream().forEach(p -> {
                            String usi = p.getUsi();
                            String spectraUsi = usi.substring(0, StringUtils.ordinalIndexOf(usi, ":", 5));
                            if(p.getSpectraUsi() == null || p.getSpectraUsi().isEmpty()) {
                                p.setSpectraUsi(spectraUsi);
                                prideMoleculesMongoService.savePsmSummaryEvidence(p);
                            }
                        });
                    }

                    taskTimeMap.put("mongoUpdatePsmStep", System.currentTimeMillis() - start);
                    return RepeatStatus.FINISHED;
                }).build();
    }

}
