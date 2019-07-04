package uk.ac.ebi.pride.archive.pipeline.jobs.services.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveRedisConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.services.redis.RedisMessageNotifier;
import uk.ac.ebi.pride.integration.message.model.impl.AssayDataGenerationPayload;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import( {ArchiveRedisConfig.class})
public class RedisClusterSimpleJob extends AbstractArchiveJob {

  @Autowired
  RedisConnectionFactory connectionFactory;

  @Autowired
  RedisMessageNotifier messageNotifier;

    /**
     * Create a connection to redis channel and send the message to the queue
     *
     * @return job
     */
    @Bean
    public Job checkRedisConfiguration() {
        return jobBuilderFactory
                .get("CheckRedisConfig")
                .start(readJedisCluster())
                .next(sendMessage())
                .build();
    }

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


    @Bean
    public Step sendMessage() {
        return stepBuilderFactory
                .get("testSendMessage")
                .tasklet(
                        (stepContribution, chunkContext) -> {

                            messageNotifier.sendNotification("archive.incoming.assay.annotation.queue",
                                    new AssayDataGenerationPayload("PXD011181", "99258"), AssayDataGenerationPayload.class);


                            return RepeatStatus.FINISHED;
                        })
                .build();
    }
}
