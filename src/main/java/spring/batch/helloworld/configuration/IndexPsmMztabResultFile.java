package spring.batch.helloworld.configuration;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.util.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import uk.ac.ebi.pride.jmztab.utils.MZTabFileParser;
import uk.ac.ebi.pride.psmindex.search.indexer.ProjectPsmsIndexer;
import uk.ac.ebi.pride.psmindex.search.service.PsmIndexService;
import uk.ac.ebi.pride.psmindex.search.service.PsmSearchService;
import uk.ac.ebi.pride.psmindex.search.service.repository.SolrPsmRepositoryFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

// todo Javadoc
// todo update with new PSM search library
// todo run only specified config
// todo don't support restartable
// todo log success/fail to Mongo
@Configuration
public class IndexPsmMztabResultFile {
  private final Logger log = LoggerFactory.getLogger(IndexPsmMztabResultFile.class);

  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Value("${redis.host}")
  private String redisServer;

  @Value("${redis.port}")
  private String redisPort;

  @Value("${pride.solr.server.psm.core.url}")
  private String solrUrl;

  JedisCluster jedisCluster;

  private String mztab;

  @Bean
  public Step getResultMztabFile() {
	return stepBuilderFactory.get("getResultMztabFile")
		.tasklet((contribution, chunkContext) -> {
		  System.out.println(">> Figure out what file to index.");
		  mztab = getStringFromRedis("todo param");
		  if (!StringUtils.isEmpty(mztab)) {
			chunkContext.getStepContext().getStepExecution().getExecutionContext().put("mztabFile", mztab);
		  }
		  return RepeatStatus.FINISHED;
		}).build();
  }

  @Bean
  public Step removeResultMztabFileRedis() {
	return stepBuilderFactory.get("removeResultMztabFileRedis")
		.tasklet((contribution, chunkContext) -> {
		  removeKeyAndValueInRedis(mztab);
		  return RepeatStatus.FINISHED;
		}).build();
  }


  @Bean
  public Step indexResultMztabFile() {
	return stepBuilderFactory.get("indexResultMztabFile")
		.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {
		  System.out.println(">> Indexing result mzTab file");
		  // todo index file (PSM)
		  SolrClient psmSolrClient = new HttpSolrClient.Builder(solrUrl).build();
		  SolrTemplate solrTemplate = new SolrTemplate(psmSolrClient);
		  SolrPsmRepositoryFactory solrPsmRepositoryFactory = new SolrPsmRepositoryFactory(solrTemplate);
		  PsmSearchService psmSearchService = new PsmSearchService(solrPsmRepositoryFactory.create());
		  PsmIndexService psmIndexService = new PsmIndexService(psmSolrClient, solrPsmRepositoryFactory.create());
		  ProjectPsmsIndexer projectPsmsIndexer = new ProjectPsmsIndexer(psmSearchService, psmIndexService);
		  String projectAccession = ""; // todo get from params
		  String resultAccession = ""; // todo get from params
		  if (psmSearchService.countByAssayAccession(resultAccession) < 1) {
			//projectPsmsIndexer.deleteAllPsmsForResultAccession(projectAccession, resultAccession); todo new delete Psms for result acc w/ paging
		  }
		  /*projectPsmsIndexer.indexAllPsmsForProjectAndAssay(
			  projectAccession,
			  projectAccession,
			  new MZTabFileParser(new File(mztab), System.out).getMZTabFile()); todo index */
		  return RepeatStatus.FINISHED;
		}).build();
  }

  @Bean
  public Step logOkIndexMztab() {
	return stepBuilderFactory.get("logOkIndexMztab")
		.tasklet((contribution, chunkContext) -> {
		  System.out.println(">> Save metrics on what was done");
		  // todo save metrics
		  return RepeatStatus.FINISHED;
		}).build();
  }

  @Bean
  public Step logFailAttemptMztab() {
	return stepBuilderFactory.get("logFailIndexMztab")
		.tasklet((contribution, chunkContext) -> {
		  System.out.println(">> Save fail metrics on mztab attempt");
		  // todo log fail metrics
		  return RepeatStatus.FINISHED;
		}).build();
  }

  @Bean
  public Step removeKeyFromRedis() {
	return stepBuilderFactory.get("removeKeyFromRedis")
		.tasklet((contribution, chunkContext) -> {
		  System.out.println(">> Save fail metrics on mztab attempt");
		  // todo log fail metrics
		  return RepeatStatus.FINISHED;
		}).build();
  }

  @Bean
  public Job transitionJobSimpleNext() {
	return jobBuilderFactory.get("transitionJobNext")
		.start(createJedisCluster())
		.next(checkProjectResultAccessions())
		.from(checkProjectResultAccessions()).on("OK").to(getResultMztabFile())
		.next(removeResultMztabFileRedis())
		.next(checkMztabPath()).on("OK").to(indexResultMztabFile())
		.next(logOkIndexMztab())
		.from(checkMztabPath()).on("FAIL").to(logFailAttemptMztab())
		.from(checkProjectResultAccessions()).on("FAIL").to(logFailAttemptMztab())
		.end()
		.build();
  }

  /**
   * Creates a new Jedis pool if one has yet to be created yet.
   */
  @Bean
  public Step createJedisCluster() {
	return stepBuilderFactory.get("createJedisCluster").tasklet((stepContribution, chunkContext) -> {
	  Set<HostAndPort> jedisClusterNodes = new HashSet<>();
	  final String STRING_SEPARATOR = "##";
	  if (redisServer.contains(STRING_SEPARATOR)) {
		String[] servers = redisServer.split(STRING_SEPARATOR);
		String[] ports;
		if (redisPort.contains(STRING_SEPARATOR)) {
		  ports = redisPort.split(STRING_SEPARATOR);
		} else {
		  ports = new String[]{redisPort};
		}
		if (ports.length!=1 && ports.length!=servers.length) {
		  log.error("Mismatch between provided Redis ports and servers. Should either have 1 port for all servers, or 1 port per server");
		}
		for (int i=0; i<servers.length; i++) {
		  String serverPort = ports.length == 1 ? ports[0] : ports[i];
		  jedisClusterNodes.add(new HostAndPort(servers[i], Integer.parseInt(serverPort)));
		  log.info("Added Jedis node: " + servers[i] + " " + serverPort);
		}
	  } else {
		jedisClusterNodes.add(new HostAndPort(redisServer, Integer.parseInt(redisPort))); //Jedis Cluster will attempt to discover cluster nodes automatically
		log.info("Added Jedis node: " + redisServer + " " + redisPort);
	  }
	  final JedisPoolConfig DEFAULT_CONFIG = new JedisPoolConfig();
	  jedisCluster = new JedisCluster(jedisClusterNodes, DEFAULT_CONFIG);
	  return RepeatStatus.FINISHED;
		}).build();
  }

  /**
   * Gets a String value for the specified Redis key.
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
  }

  /**
   * Removes a key (or more) and the corresponding value(s) from Redis.
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
		return new FlowExecutionStatus(StringUtils.isEmpty(stepExecution.getExecutionContext().get("mztabFile")) ?
			"FAIL" : "OK");
	}
  }

  public static class ValidParametersDecider implements JobExecutionDecider {

	@Override
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
	  // todo project and result accession numbers present from job parameters
	  return new FlowExecutionStatus(StringUtils.isEmpty("projAcc") || StringUtils.isEmpty("resultAcc") ?
		  "FAIL" : "OK");
	}
  }
}
