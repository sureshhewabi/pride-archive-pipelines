package uk.ac.ebi.pride.archive.pipeline.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.hibernate5.HibernateExceptionTranslator;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = {"uk.ac.ebi.pride.archive.repo"})
@Slf4j
public class ArchiveDataSource { // todo unit tests, Javadoc

  @Value("${spring.datasource.oracle.jdbcUrl}")
  private String springArchiveJdbcUrl;

  @Value("${spring.datasource.oracle.username}")
  private String springArchiveUsername;

  @Value("${spring.datasource.oracle.password}")
  private String springArchivePassword;

  @Value("${spring.datasource.oracle.driver.class.name}")
  private String springArchiveDriverClassname;

  @Bean(name = "dataSourceOracle")
  public DataSource dataSource() {
    return DefaultBatchConfigurer.createDatasource(
        springArchiveDriverClassname,
        springArchiveJdbcUrl,
        springArchiveUsername,
        springArchivePassword);
  }

  @Bean(name = "entityManagerFactory")
  public LocalContainerEntityManagerFactoryBean entityManagerFactory(
      EntityManagerFactoryBuilder builder, @Qualifier("dataSourceOracle") DataSource dataSource) {
    return builder
        .dataSource(dataSource)
        .packages("uk.ac.ebi.pride.archive.repo.repos")
        .persistenceUnit("Project")
        .persistenceUnit("ProjectFile")
        .build();
  }

  @Bean(name = "jpaTransactionManager")
  public JpaTransactionManager jpaTransactionManager(
      @Qualifier("entityManagerFactory") EntityManagerFactory entityManagerFactory) {
    return new JpaTransactionManager(entityManagerFactory);
  }

  @Bean
  public HibernateExceptionTranslator hibernateExceptionTranslator() {
    return new HibernateExceptionTranslator();
  }
}
