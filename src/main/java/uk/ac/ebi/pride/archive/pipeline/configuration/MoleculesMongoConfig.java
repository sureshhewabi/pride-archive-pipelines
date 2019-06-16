package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import uk.ac.ebi.pride.mongodb.configs.AbstractPrideMongoConfiguration;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages = {"uk.ac.ebi.pride.mongodb.molecules.service"})
@EnableMongoRepositories(basePackages = {"uk.ac.ebi.pride.mongodb.molecules.repo"}, mongoTemplateRef="moleculesMongoTemplate")
//@EnableAutoConfiguration
public class MoleculesMongoConfig extends AbstractPrideMongoConfiguration {

    @Value("${mongodb.molecules.database}")
    private String mongoProjectDatabase;

    @Value("${mongodb.projects.machine.uri}")
    private String mongoURI;

    @Override
    @Bean(name = "moleculesMongoTemplate")
    public MongoTemplate mongoTemplate() {
        return new MongoTemplate(mongoDbFactory());
    }

    @Override
    protected String getDatabaseName() {
        return mongoProjectDatabase;
    }

    @Override
    public String getMongoURI() {
        return mongoURI;
    }
}