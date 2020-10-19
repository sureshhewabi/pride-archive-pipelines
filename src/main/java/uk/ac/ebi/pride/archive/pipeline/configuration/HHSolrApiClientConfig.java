package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.solr.api.client.SolrApiClientFactory;
import uk.ac.ebi.pride.solr.api.client.SolrProjectClient;


@Configuration
public class HHSolrApiClientConfig {

    private final SolrApiClientFactory solrApiClientFactory;

    public HHSolrApiClientConfig(@Value("${solr.api.hh.baseUrl}") String apiBaseUrl,
                                 @Value("${solr.api.hh.keyName}") String apiKeyName,
                                 @Value("${solr.api.hh.keyValue}") String apiKeyValue,
                                 @Value("${spring.application.name}") String appName) {

        this.solrApiClientFactory = new SolrApiClientFactory(apiBaseUrl, apiKeyName, apiKeyValue, appName);
    }

    @Bean
    @Qualifier("solrProjectClientHH")
    public SolrProjectClient solrProjectClientHH() {
        return solrApiClientFactory.getSolrProjectClient();
    }

}
