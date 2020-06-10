package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import uk.ac.ebi.ddi.downloas.logs.ElasticSearchWsClient;
import uk.ac.ebi.ddi.downloas.logs.ElasticSearchWsConfigProd;

/**
 * This class contains the configuration to get connected to
 * ElasticSearch to read log files
 *
 * @author Suresh Hewapathirana
 */
@Configuration
@PropertySource("classpath:application.properties")
public class ElasticSearchWsConfig {

    @Value("${elasticsearch.port}")
    private Integer port;
    @Value("${elasticsearch.host}")
    private String host;
    @Value("${elasticsearch.username}")
    private String username;
    @Value("${elasticsearch.password}")
    private String password;

    @Bean
    ElasticSearchWsConfigProd elasticSearchWsConfigProd(){
        return new ElasticSearchWsConfigProd(port,host,username, password);
    }

    @Bean
    ElasticSearchWsClient elasticSearchClient(ElasticSearchWsConfigProd elasticSearchWsConfigProd){
        return new ElasticSearchWsClient(elasticSearchWsConfigProd);
    }
}
