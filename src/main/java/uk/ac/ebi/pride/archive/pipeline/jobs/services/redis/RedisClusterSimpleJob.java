package uk.ac.ebi.pride.archive.pipeline.jobs.services.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveRedisConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.services.RedisMessageNotifier;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.integration.message.model.FileType;
import uk.ac.ebi.pride.integration.message.model.impl.FileGenerationPayload;

import java.util.HashSet;
import java.util.Set;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import( {ArchiveRedisConfig.class})
public class RedisClusterSimpleJob extends AbstractArchiveJob {

  @Autowired
  RedisConnectionFactory connectionFactory;

  @Autowired
  RedisMessageNotifier messageNotifier;

  /** Creates a new Jedis pool if one has yet to be created yet. */
  @Bean
  public Step readJedisCluster() {
    return stepBuilderFactory
        .get("createJedisCluster")
        .tasklet(
            (stepContribution, chunkContext) -> {
                Long clusterSize = connectionFactory.getClusterConnection().clusterGetClusterInfo().getClusterSize();
                log.info("The cluster size is: " + clusterSize);
              return RepeatStatus.FINISHED;
            })
        .build();
  }

    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job checkRedisConfiguration() {
        return jobBuilderFactory
                .get("CheckRedisConfig")
                .start(readJedisCluster())
                .next(sendMessage())
                .build();
    }

    @Bean
    public Step sendMessage() {
        return stepBuilderFactory
                .get("testSendMessage")
                .tasklet(
                        (stepContribution, chunkContext) -> {
                            messageNotifier.sendNotification("archive.mgf.completion.queue", new FileGenerationPayload("/mgf/mgf.mgf", FileType.MGF), FileGenerationPayload.class);
                            return RepeatStatus.FINISHED;
                        })
                .build();
    }
}
