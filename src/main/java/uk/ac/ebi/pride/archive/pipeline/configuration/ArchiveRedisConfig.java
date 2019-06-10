package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Set;

@Configuration
@EnableTransactionManagement
@EnableAutoConfiguration
public class ArchiveRedisConfig extends RedisClusterConfiguration {

    @Override
    public Set<RedisNode> getClusterNodes() {
        return super.getClusterNodes();
    }
}
