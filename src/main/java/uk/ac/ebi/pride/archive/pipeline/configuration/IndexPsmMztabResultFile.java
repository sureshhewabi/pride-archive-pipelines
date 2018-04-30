package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import uk.ac.ebi.pride.archive.repo.file.ProjectFileRepository;
import uk.ac.ebi.pride.archive.repo.project.ProjectRepository;
import uk.ac.ebi.pride.jmztab.model.MZTabFile;
import uk.ac.ebi.pride.jmztab.utils.MZTabFileParser;
import uk.ac.ebi.pride.psmindex.mongo.search.indexer.MongoProjectPsmIndexer;
import uk.ac.ebi.pride.psmindex.mongo.search.service.MongoPsmIndexService;
import uk.ac.ebi.pride.psmindex.mongo.search.service.MongoPsmSearchService;
import uk.ac.ebi.pride.psmindex.mongo.search.service.repository.MongoPsmRepository;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

// todo Javadoc
// todo don't support restartable
// todo log success/fail to Mongo
// todo parent job to launch indexing PSMs of result file
// todo repeat this for PSMs
// todo repeat this for Spectra
// todo migrate this functionality from old submission pipeline to this submission pipeline (e.g.
// update submission pipeline, update archive-integration)
@Configuration
@EnableBatchProcessing
@ComponentScan(
  basePackageClasses = {
    DefaultBatchConfigurer.class,
    MongoDataSource.class,
    ArchiveDataSource.class
  }
)
public class IndexPsmMztabResultFile {
  private final Logger log = LoggerFactory.getLogger(IndexPsmMztabResultFile.class);

  @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
  @Autowired
  private ProjectRepository projectRepository;

  @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
  @Autowired
  private ProjectFileRepository projectFileRepository;

  @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
  @Autowired
  private MongoPsmRepository mongoPsmRepository;

  private MongoProjectPsmIndexer mongoProjectPsmIndexer;

  @Value("${redis.host}")
  private String redisServer;

  @Value("${redis.port}")
  private String redisPort;

  @Value("${pride.solr.server.psm.core.url}")
  private String solrUrl;

  private JedisCluster jedisCluster;

  private String mztab;

  @Bean
  public Step getResultMztabFile() {
    return stepBuilderFactory
        .get("getResultMztabFile")
        .tasklet(
            (contribution, chunkContext) -> {
              log.info(">> Figure out what file to index."); // todo
              mztab = getStringFromRedis("todo param");
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

  /*  @Bean
  public Step removeResultMztabFileRedis() {
    return stepBuilderFactory
        .get("removeResultMztabFileRedis")
        .tasklet(
            (contribution, chunkContext) -> {
              removeKeyAndValueInRedis(mztab);
              return RepeatStatus.FINISHED;
            })
        .build();
  } // not required, key should auto-expire todo refactor*/

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
              // todo index file (PSM) for Solr?
              /* SolrClient psmSolrClient = new HttpSolrClient.Builder(solrUrl).build();
              SolrTemplate solrTemplate = new SolrTemplate(psmSolrClient);
              SolrPsmRepositoryFactory solrPsmRepositoryFactory =
                  new SolrPsmRepositoryFactory(solrTemplate);
              PsmSearchService psmSearchService =
                  new PsmSearchService(solrPsmRepositoryFactory.create());
              PsmIndexService psmIndexService =
                  new PsmIndexService(psmSolrClient, solrPsmRepositoryFactory.create());
              ProjectPsmsIndexer projectPsmsIndexer =
                  new ProjectPsmsIndexer(psmSearchService, psmIndexService);
              if (psmSearchService.countByAssayAccession(resultAccession) < 1) {
                // projectPsmsIndexer.deleteAllPsmsForResultAccession(projectAccession,
                // resultAccession); todo new delete Psms for result acc w/ paging
              }*/
              /*projectPsmsIndexer.indexAllPsmsForProjectAndAssay(
              projectAccession,
              projectAccession,
              ;*/
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
              System.out.println(">> Save fail metrics on mztab attempt");
              // todo log fail metrics
              return RepeatStatus.FINISHED;
            })
        .build();
  }

  @Bean
  public Step removeKeyFromRedis() {
    return stepBuilderFactory
        .get("removeKeyFromRedis")
        .tasklet(
            (contribution, chunkContext) -> {
              System.out.println(">> Save fail metrics on mztab attempt");
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
        .next(createJedisCluster())
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

  /** Creates a new Jedis pool if one has yet to be created yet. */
  @Bean
  public Step createJedisCluster() {
    return stepBuilderFactory
        .get("createJedisCluster")
        .tasklet(
            (stepContribution, chunkContext) -> {
              Set<HostAndPort> jedisClusterNodes = new HashSet<>();
              final String STRING_SEPARATOR = "##";
              if (redisServer.contains(STRING_SEPARATOR)) {
                String[] servers = redisServer.split(STRING_SEPARATOR);
                String[] ports;
                if (redisPort.contains(STRING_SEPARATOR)) {
                  ports = redisPort.split(STRING_SEPARATOR);
                } else {
                  ports = new String[] {redisPort};
                }
                if (ports.length != 1 && ports.length != servers.length) {
                  log.error(
                      "Mismatch between provided Redis ports and servers. Should either have 1 port for all servers, or 1 port per server");
                }
                for (int i = 0; i < servers.length; i++) {
                  String serverPort = ports.length == 1 ? ports[0] : ports[i];
                  jedisClusterNodes.add(new HostAndPort(servers[i], Integer.parseInt(serverPort)));
                  log.info("Added Jedis node: " + servers[i] + " " + serverPort);
                }
              } else {
                jedisClusterNodes.add(new HostAndPort(redisServer, Integer.parseInt(redisPort)));
                // Jedis Cluster will attempt to discover cluster nodes automatically
                log.info("Added Jedis node: " + redisServer + " " + redisPort);
              }
              final JedisPoolConfig DEFAULT_CONFIG = new JedisPoolConfig();
              jedisCluster = new JedisCluster(jedisClusterNodes, DEFAULT_CONFIG);
              return RepeatStatus.FINISHED;
            })
        .build();
  } // todo refactor to separate config class

  /**
   * Gets a String value for the specified Redis key.
   *
   * @param key the key to use
   * @return String the key's value
   */
  private String getStringFromRedis(String key) {
    String result = "";
    try {
      log.info("Connecting to Redis.");
      log.info("Getting contents to Redis for key: " + key);
      if (!jedisCluster.exists(key)) {
        log.error("Redis does not hold a value for key: " + key);
      } else {
        result = jedisCluster.get(key);
        log.info("Successfully gotten value from key: " + result);
      }
    } catch (Exception e) {
      log.error("Exception while getting value to Redis for key: " + key, e);
    }
    return result;
  } // todo refactor to separate config class

  /**
   * Removes a key (or more) and the corresponding value(s) from Redis.
   *
   * @param keyToRemove the key to remove
   */
  private void removeKeyAndValueInRedis(String keyToRemove) {
    try {
      log.info("Connecting to Redis.");
      log.info("Removing Redis key: " + keyToRemove);
      long numberOfKeysRemoved = jedisCluster.del(keyToRemove);
      if (numberOfKeysRemoved < 1) {
        log.info("Redis does not hold a value for key: " + keyToRemove);
      } else {
        log.info("Successfully removed key: " + keyToRemove);
      }
    } catch (Exception e) {
      log.error("Exception while removing in Redis for key: " + keyToRemove, e);
    }
  } // todo refactor to separate config class

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
