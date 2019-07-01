package uk.ac.ebi.pride.archive.pipeline.jobs.molecules;

import de.mpc.pia.modeller.PIAModeller;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.jobs.services.redis.RedisClusterSimpleJob;
import uk.ac.ebi.pride.archive.pipeline.services.pia.PIAModelerService;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.spectra.configs.AWS3Configuration;
import uk.ac.ebi.pride.archive.spectra.services.S3SpectralArchive;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoPrideAssay;
import uk.ac.ebi.pride.mongodb.archive.repo.files.PrideFileMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;

@Configuration
@Slf4j
@Import({MoleculesMongoConfig.class, AWS3Configuration.class})
public class RunPrideClusterJob extends AbstractArchiveJob {

  private static final Long MERGE_FILE_ID = 1L;
  private static final Long FILE_ID = 1L;

  @Autowired
  S3SpectralArchive spectralArchive;

  @Autowired
  PrideMoleculesMongoService moleculesService;

  @Value("${pride.data.prod.directory}")
  String productionPath;

//db.pride_peptide_evidences.aggregate(
//        [
//  { $unwind : "$psmAccessions"},
//  {
//    "$group": {
//    "_id": {
//      "charge": "$psmAccessions.charge",
//              "mz": {$trunc: "$psmAccessions.precursorMass"},
//    },
//    peptides: { $push: "$$ROOT.psmAccessions.usi" },
//    "count": { "$sum": 1 }
//  },
//
//  }, {$sort:{"count":-1}}
//], {'allowDiskUse':true})


  /**
   * Defines the job to Sync all the projects from OracleDB into MongoDB database.
   *
   * @return the calculatePrideArchiveDataUsage job
   */
  @Bean
  public Job analyzeAssayInformation() {
//    return jobBuilderFactory
//            .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_ASSAY_ANALYSIS.getName())
//            .start(analyzeAssayInformationStep())
//            .next(updateAssayInformationStep())
//            .next(indexSpectraStep())
//            .next(proteinPeptideIndexStep())
//            .build();
    return null;
  }

}
