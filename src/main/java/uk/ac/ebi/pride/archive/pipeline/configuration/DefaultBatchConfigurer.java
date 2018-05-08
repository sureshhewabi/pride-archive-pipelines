package uk.ac.ebi.pride.archive.pipeline.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;
import java.sql.Driver;

@Configuration
@Slf4j
public class DefaultBatchConfigurer { // todo unit tests, Javadoc

  @Value("${spring.datasource.driver.class.name}")
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
    return createDatasource(springDataSourceDriverClassName, springDataSourceUrl, springDataSourceUsername, springDataSourcePassword);
  }

  public static DataSource createDatasource(String driverClassName, String url, String username, String password) {
    final SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
    try {
      dataSource.setDriver((Driver) Class.forName(driverClassName).newInstance());
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      log.error("Unable to find class / Illegal Access / Unable to instantiate", e);
    }
    dataSource.setUrl(url);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    return dataSource;
  }
}