package uk.ac.ebi.pride.archive.pipeline.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import uk.ac.ebi.pride.archive.pipeline.utility.RedisUtilities;
import uk.ac.ebi.pride.archive.repo.file.ProjectFileRepository;
import uk.ac.ebi.pride.archive.repo.project.ProjectRepository;
import uk.ac.ebi.pride.jmztab.model.MZTabFile;
import uk.ac.ebi.pride.jmztab.utils.MZTabFileParser;
import uk.ac.ebi.pride.psmindex.mongo.search.indexer.MongoProjectPsmIndexer;
import uk.ac.ebi.pride.psmindex.mongo.search.service.MongoPsmIndexService;
import uk.ac.ebi.pride.psmindex.mongo.search.service.MongoPsmSearchService;
import uk.ac.ebi.pride.psmindex.mongo.search.service.repository.MongoPsmRepository;

import java.io.File;

// todo Javadoc
// todo don't support restartable
// todo log success/fail to Mongo
// todo parent job to launch indexing PSMs of result file
// todo repeat this for PSMs
// todo repeat this for Spectra
// todo migrate this functionality from old submission pipeline to this submission pipeline (e.g.
// update submission pipeline, update archive-integration)
@SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
@Configuration
@EnableBatchProcessing
@Slf4j
@ComponentScan(
  basePackageClasses = {
    DefaultBatchConfigurer.class,
    MongoDataSource.class,
    ArchiveDataSource.class,
    RedisClusterConfigurer.class
  }
)
public class IndexMztabResultFileForPsm {

  @Autowired private JobBuilderFactory jobBuilderFactory;

  @Autowired private StepBuilderFactory stepBuilderFactory;

  @Autowired private ProjectRepository projectRepository;

  @Autowired private ProjectFileRepository projectFileRepository;

  @Autowired private MongoPsmRepository mongoPsmRepository;

  private MongoProjectPsmIndexer mongoProjectPsmIndexer;

  private String mztab;

  @Autowired private RedisClusterConfigurer redisClusterConfigurer;

  @Bean
  public Step getResultMztabFile() {
    return stepBuilderFactory
        .get("getResultMztabFile")
        .tasklet(
            (contribution, chunkContext) -> {
              log.info(">> Figure out what file to index."); // todo
              mztab =
                  RedisUtilities.getStringFromRedis(
                      "todo param", redisClusterConfigurer.getJedisCluster());
              if (!StringUtils.isEmpty(mztab)) {
                chunkContext
                    .getStepContext()
                    .getStepExecution()
                    .getExecutionContext()
                    .put("mztabFile", mztab);
              }
              return RepeatStatus.FINISHED;
            })
        .build();
  }

  @Bean
  public Step setupMongoProjectPsmIndexer() {
    return stepBuilderFactory
        .get("setupMongoProjectPsmIndexer")
        .tasklet(
            (contribution, chunkContext) -> {
              MongoPsmIndexService mongoPsmIndexService = new MongoPsmIndexService();
              mongoPsmIndexService.setMongoPsmRepository(mongoPsmRepository);
              MongoPsmSearchService mongoPsmSearchService = new MongoPsmSearchService();
              mongoPsmSearchService.setMongoPsmRepository(mongoPsmRepository);
              mongoProjectPsmIndexer =
                  new MongoProjectPsmIndexer(mongoPsmIndexService, mongoPsmSearchService);
              return RepeatStatus.FINISHED;
            })
        .build();
  }

  @Bean
  public Step indexResultMztabFile() {
    return stepBuilderFactory
        .get("indexResultMztabFile")
        .tasklet(
            (StepContribution contribution, ChunkContext chunkContext) -> {
              System.out.println(">> Indexing result mzTab file");
              String projectAccession = ""; // todo get from params
              String resultAccession = ""; // todo get from params
              MZTabFile mzTabFile = new MZTabFileParser(new File(mztab), System.out).getMZTabFile();
              mongoProjectPsmIndexer.deleteAllPsmsForAssay(resultAccession);
              try {
                mongoProjectPsmIndexer.indexAllPsmsForProjectAndAssay(
                    projectAccession, resultAccession, mzTabFile);
              } catch (Exception e) {
                log.error(
                    "Problem indexing mzTab file for: "
                        + projectAccession
                        + " "
                        + resultAccession
                        + " "
                        + mztab);
                mongoProjectPsmIndexer.deleteAllPsmsForAssay(resultAccession);
              }
              return RepeatStatus.FINISHED;
            })
        .build();
  }

  @Bean
  public Step logOkIndexMztab() {
    return stepBuilderFactory
        .get("logOkIndexMztab")
        .tasklet(
            (contribution, chunkContext) -> {
              System.out.println(">> Save metrics on what was done");
              // todo save metrics
              return RepeatStatus.FINISHED;
            })
        .build();
  }

  @Bean
  public Step logFailAttemptMztab() {
    return stepBuilderFactory
        .get("logFailIndexMztab")
        .tasklet(
            (contribution, chunkContext) -> {
              log.info(">> Save fail metrics on mztab attempt");
              // todo log fail metrics
              return RepeatStatus.FINISHED;
            })
        .build();
  }

  @Bean
  public Job transitionJobSimpleNext() {
    return jobBuilderFactory
        .get("resultPsmIndexing")
        .start(setupMongoProjectPsmIndexer())
        .next(redisClusterConfigurer.createJedisCluster())
        .next(checkProjectResultAccessions())
        .from(checkProjectResultAccessions())
        .on("OK")
        .to(getResultMztabFile())
        .next(checkMztabPath())
        .on("OK")
        .to(indexResultMztabFile())
        .next(logOkIndexMztab())
        .from(checkMztabPath())
        .on("FAIL")
        .to(logFailAttemptMztab())
        .from(checkProjectResultAccessions())
        .on("FAIL")
        .to(logFailAttemptMztab())
        .end()
        .build();
  }

  @Bean
  public ValidStringDecider checkMztabPath() {
    return new ValidStringDecider();
  }

  @Bean
  public ValidParametersDecider checkProjectResultAccessions() {
    return new ValidParametersDecider();
  }

  public static class ValidStringDecider implements JobExecutionDecider {

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
      return new FlowExecutionStatus(
          StringUtils.isEmpty(stepExecution.getExecutionContext().get("mztabFile"))
              ? "FAIL"
              : "OK");
    }
  }

  public static class ValidParametersDecider implements JobExecutionDecider {

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
      // todo project and result accession numbers present from job parameters
      return new FlowExecutionStatus(
          StringUtils.isEmpty("projAcc") || StringUtils.isEmpty("resultAcc") ? "FAIL" : "OK");
    }
  }
}
