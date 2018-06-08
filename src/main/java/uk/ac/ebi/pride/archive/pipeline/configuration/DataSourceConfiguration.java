package uk.ac.ebi.pride.archive.pipeline.configuration;

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
 * Created by ypriverol (ypriverol@gmail.com) on 08/06/2018.
 */

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;

import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/**
 * @author Dave Syer
 *
 */
@Configuration
@PropertySource("classpath:/batch-h2.properties")
public class DataSourceConfiguration {

    @Autowired
    private Environment environment;

    @Autowired
    private ResourceLoader resourceLoader;

    @PostConstruct
    protected void initialize() {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(resourceLoader.getResource(environment.getProperty("batch.schema.script")));
        populator.setContinueOnError(true);
        DatabasePopulatorUtils.execute(populator , dataSource());
    }

    @Bean(name = "hsqlDatraSource", destroyMethod="close")
    @Primary
    public DataSource dataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(environment.getProperty("batch.jdbc.driver"));
        dataSource.setUrl(environment.getProperty("batch.jdbc.url"));
        dataSource.setUsername(environment.getProperty("batch.jdbc.user"));
        dataSource.setPassword(environment.getProperty("batch.jdbc.password"));
        return dataSource;
    }

}