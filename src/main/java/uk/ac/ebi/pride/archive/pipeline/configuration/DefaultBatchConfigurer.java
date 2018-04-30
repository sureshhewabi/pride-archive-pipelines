package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;
import java.sql.Driver;

@Configuration
public class DefaultBatchConfigurer { // todo unit tests, Javadoc

  private final Logger log = LoggerFactory.getLogger(DefaultBatchConfigurer.class);

  @Value("${spring.datasource.driver-class-name}")
  private String springDataSourceDriverClassName;

  @Value("${spring.datasource.url}")
  private String springDataSourceUrl;

  @Value("${spring.datasource.username}")
  private String springDataSourceUsername;

  @Value("${spring.datasource.password}")
  private String springDataSourcePassword;

  @Bean
  @Primary
  public DataSource hsqldbDataSource() {
    final SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
    try {
      dataSource.setDriver((Driver) Class.forName(springDataSourceDriverClassName).newInstance());
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      log.error("Unable to find class / Illegal Access / Unable to instantiate", e);
    }
    dataSource.setUrl(springDataSourceUrl);
    dataSource.setUsername(springDataSourceUsername);
    dataSource.setPassword(springDataSourcePassword);
    return dataSource;
  }
}
