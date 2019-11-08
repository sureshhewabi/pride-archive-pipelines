package uk.ac.ebi.pride.archive.pipeline.jobs.stats;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.spectra.configs.AWS3Configuration;
import uk.ac.ebi.pride.mongodb.archive.model.stats.MongoPrideStats;
import uk.ac.ebi.pride.mongodb.archive.model.stats.PrideStatsKeysConstants;
import uk.ac.ebi.pride.mongodb.archive.service.stats.PrideStatsMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;

import java.util.*;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, MoleculesMongoConfig.class, DataSourceConfiguration.class})
public class PrideArchiveMoleculeReportJob extends AbstractArchiveJob {



    final PrideMoleculesMongoService moleculesService;
    final PrideStatsMongoService prideStatsMongoService;

    Date date;

    @Autowired
    public PrideArchiveMoleculeReportJob(PrideMoleculesMongoService moleculesService, PrideStatsMongoService prideStatsMongoService) {
        this.moleculesService = moleculesService;
        this.prideStatsMongoService = prideStatsMongoService;
    }

    /**
     * All the stats are compute at an specific time 00:00:00
     *
     */
    @Autowired
    public void initDate() {
        Calendar now = Calendar.getInstance();
        now.set(Calendar.HOUR, 0);
        now.set(Calendar.MINUTE, 0);
        now.set(Calendar.SECOND, 0);
        now.set(Calendar.HOUR_OF_DAY, 0);
        this.date = now.getTime();
    }

    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job moleculeStatsJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_MOLECULE_STATS.getName())
                .start(computeMoleculesStats())
                .build();
    }

    @Bean
    public Step computeMoleculesStats() {
        return stepBuilderFactory
                .get("computeMoleculesStats")
                .tasklet((stepContribution, chunkContext) -> {
                    long proteinEvidences = moleculesService.getNumberProteinEvidences();
                    long peptideEvidecnes = moleculesService.getNumberPeptideEvidences();
                    long psmEvidences     = moleculesService.getNumberPSMEvidecnes();
                    Map<String, List<Tuple<String, Integer>>> moleculesMap = new HashMap<>();
                    List<Tuple<String, Integer>> moleculesStats = new ArrayList<>();
                    moleculesStats.add(new Tuple<String, Integer>("Number protein evidences", Math.toIntExact(proteinEvidences)));
                    moleculesStats.add(new Tuple<String, Integer>("Number protein peptide evidences",  Math.toIntExact(peptideEvidecnes)));
                    moleculesStats.add(new Tuple<String, Integer>("Number psm evidences",  Math.toIntExact(psmEvidences)));
                    moleculesMap.put(PrideStatsKeysConstants.EVIDENCES_IN_ARCHIVE.statsKey, moleculesStats);

                    MongoPrideStats stats = MongoPrideStats.builder()
                            .submissionsCount(moleculesMap)
                            .estimationDate(date).build();
                    prideStatsMongoService.save(stats);

                    return RepeatStatus.FINISHED;
                }).build();
    }


}
