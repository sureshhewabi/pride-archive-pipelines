package uk.ac.ebi.pride.archive.pipeline.jobs.services.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DefaultBatchConfigurer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;

import java.util.HashSet;
import java.util.Set;

@Configuration
@Slf4j
@EnableBatchProcessing
@ComponentScan(basePackageClasses = {DefaultBatchConfigurer.class})
public class RedisClusterConfigurer extends AbstractArchiveJob {

  @Value("${redis.host}")
  private String redisServer;

  @Value("${redis.port}")
  private String redisPort;

  private JedisCluster jedisCluster;

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
  }

  /**
   * Gets jedisCluster.
   *
   * @return Value of jedisCluster.
   */
  public JedisCluster getJedisCluster() {
    return jedisCluster;
  }

  /**
   * Sets new jedisCluster.
   *
   * @param jedisCluster New value of jedisCluster.
   */
  public void setJedisCluster(JedisCluster jedisCluster) {
    this.jedisCluster = jedisCluster;
  }
}
