package uk.ac.ebi.pride.archive.pipeline.jobs.services.solrcloud;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 *
 * This class Configure the PRIDE Archive SolrCloud instance, creating the collection, deleting existing collection,
 * creating the Archive Collection and Refining it.
 *
 * <p>
 * @author ypriverol
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@EnableConfigurationProperties
public class PrideArchiveSolrCloudMasterConfigJob extends AbstractConfigureSolrCloudClusterJob{

    @Value("${solr.master.hx.url}")
    private String solrMasterURL;

    @Override
    String getSolrMasterURL() {
        return solrMasterURL;
    }
}
