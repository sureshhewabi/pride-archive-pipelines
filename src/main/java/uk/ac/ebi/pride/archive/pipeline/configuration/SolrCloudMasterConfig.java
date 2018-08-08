package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.repository.config.EnableSolrRepositories;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 13/06/2018.
 */
@Configuration
@EnableSolrRepositories(basePackages = "uk.ac.ebi.pride.solr.indexes.pride.repository")
@ComponentScan(basePackages = "uk.ac.ebi.pride.solr.indexes.pride.services")
public class SolrCloudMasterConfig {

    @Value("${spring.pridedb.solr.hx.url}")
    private String solrURls;

    @Bean
    public SolrClient solrClient() {
        List<String> urls = Arrays.stream(solrURls.split(",")).map(String::trim).collect(Collectors.toList());
        return new CloudSolrClient.Builder().withSolrUrl(urls).build();
    }
    @Bean
    public SolrTemplate solrTemplate(SolrClient solrClient) {
        return new SolrTemplate(solrClient);
    }

}
