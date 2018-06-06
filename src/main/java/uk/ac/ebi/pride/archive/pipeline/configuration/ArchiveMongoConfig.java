package uk.ac.ebi.pride.archive.pipeline.configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import uk.ac.ebi.pride.mongodb.configs.AbstractPrideMongoConfiguration;

/**
 * Configures the Mongo-based repositories. For details, see:
 * https://docs.spring.io/spring-data/mongodb/docs/2.1.0.BUILD-SNAPSHOT/reference/html/
 */
@Configuration
@EnableMongoRepositories(basePackages = {"uk.ac.ebi.pride.mongodb.archive.repo.projects"})

public class ArchiveMongoConfig extends AbstractPrideMongoConfiguration { // todo unit tests, Javadoc

  @Value("${mongodb.project.database}")
  private String mongoProjectDatabase;

  @Value("${mongodb.project.app.user}")
  private String user;

  @Value("${mongodb.project.app.password}")
  private String password;

  @Value("${mongodb.project.app.authenticationDatabase}")
  private String authenticationDatabse;

  @Value("${mongodb.projects.replicate.hosts}")
  private String mongoHosts;

  @Value("${mongodb.projects.replicate.ports}")
  private String mongoPorts;

  @Value("${mongodb.project.app.machine.port}")
  private String port;

  @Value("${mongo.single.machine}")
  private String singleMachine;

  @Value("${mongodb.projects.single.machine.host}")
  private String mongoHost;

  @Override
  protected String getDatabaseName() {
    return mongoProjectDatabase;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public String getAuthenticationDatabse() {
    return authenticationDatabse;
  }

  @Override
  public String getMongoHosts() {
    return mongoHosts;
  }

  @Override
  public String getMongoPorts() {
    return mongoPorts;
  }

  @Override
  public String getPort() {
    return port;
  }

  @Override
  public String getSingleMachine() {
    return singleMachine;
  }

  @Override
  public String getMongoHost() {
    return mongoHost;
  }
}
