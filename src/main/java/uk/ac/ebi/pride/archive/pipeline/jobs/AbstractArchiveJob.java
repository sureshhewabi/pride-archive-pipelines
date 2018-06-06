package uk.ac.ebi.pride.archive.pipeline.jobs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import uk.ac.ebi.pride.archive.pipeline.configuration.DefaultBatchConfigurer;

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
 * This class create the default Job and Step handler for every job.
 *
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 05/06/2018.
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@Import({DefaultBatchConfigurer.class})
public class AbstractArchiveJob {

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    protected JobBuilderFactory jobBuilderFactory;

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    protected StepBuilderFactory stepBuilderFactory;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
