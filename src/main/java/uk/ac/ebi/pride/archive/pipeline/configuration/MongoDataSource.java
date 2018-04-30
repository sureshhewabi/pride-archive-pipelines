package uk.ac.ebi.pride.archive.pipeline.configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * Configures the Mongo-based repositories. For details, see:
 * https://docs.spring.io/spring-data/mongodb/docs/2.1.0.BUILD-SNAPSHOT/reference/html/
 */
@Configuration
@EnableMongoRepositories(
  basePackages = {"uk.ac.ebi.pride.psmindex.mongo.search.service.repository"}
)
public class MongoDataSource extends AbstractMongoConfiguration { // todo unit tests, Javadoc

  @Value("${spring.datasource.mongodb.uri}")
  String mongoUri;

  @Value("${spring.datasource.mongodb.database}")
  String mongoDatabase;

  /**
   * Instantiates the Mongo Client
   *
   * @return the Mongo Client
   */
  @SuppressWarnings("NullableProblems")
  @Override
  public MongoClient mongoClient() {
    return new MongoClient(new MongoClientURI(mongoUri));
  }

  /**
   * Gets the database name
   *
   * @return the database name
   */
  @SuppressWarnings("NullableProblems")
  @Override
  protected String getDatabaseName() {
    return mongoDatabase;
  }
}
