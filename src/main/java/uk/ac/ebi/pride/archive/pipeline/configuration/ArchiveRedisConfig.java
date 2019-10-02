package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages = {"uk.ac.ebi.pride.archive.pipeline.services"})
@EnableAutoConfiguration
public class ArchiveRedisConfig extends RedisClusterConfiguration {

    @Value("${redis.host.one}")
    private String hostOne;
    @Value("${redis.port.one}")
    private String portOne;

    @Value("${redis.host.two}")
    private String hostTwo;
    @Value("${redis.port.two}")
    private String portTwo;

    @Value("${redis.host.three}")
    private String hostThree;
    @Value("${redis.port.three}")
    private String portThree;

    @Value("${redis.host.four}")
    private String hostFour;
    @Value("${redis.port.four}")
    private String portFour;

    @Value("${redis.host.five}")
    private String hostFive;
    @Value("${redis.port.five}")
    private String portFive;

    @Value("${redis.host.six}")
    private String hostSix;
    @Value("${redis.port.six}")
    private String portSix;

    @Bean
    public RedisConnectionFactory connectionFactory(){

        List<String> nodes = new ArrayList<>();

        nodes = addNode(hostOne , portOne, nodes);
        nodes = addNode(hostTwo,portTwo, nodes);
        nodes = addNode(hostThree, portThree, nodes);
        nodes = addNode(hostFour, portFour, nodes);
        nodes = addNode(hostFive, portFive, nodes);
        nodes = addNode(hostSix, portSix, nodes);
        RedisClusterConfiguration clusterConf = new RedisClusterConfiguration(nodes);

        return new JedisConnectionFactory(clusterConf);
    }

    private List<String> addNode(String host, String port, List<String> nodes){
        String masterNode = host + ":" + port;
        nodes.add(masterNode);
        return nodes;
    }

    @Override
    public Set<RedisNode> getClusterNodes() {
        return super.getClusterNodes();
    }
}
