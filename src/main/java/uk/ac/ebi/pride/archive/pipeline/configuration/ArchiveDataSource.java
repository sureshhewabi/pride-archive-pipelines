package uk.ac.ebi.pride.archive.pipeline.configuration;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.hibernate5.HibernateExceptionTranslator;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = {"uk.ac.ebi.pride.archive.repo"})
public class ArchiveDataSource { // todo unit tests, Javadoc

  @Bean(name = "dataSourceOracle")
  @ConfigurationProperties(prefix = "spring.datasource.oracle")
  public DataSource dataSource() {
    return DataSourceBuilder.create().build();
  }

  @Bean(name = "entityManagerFactory")
  public LocalContainerEntityManagerFactoryBean entityManagerFactory(
      EntityManagerFactoryBuilder builder, @Qualifier("dataSourceOracle") DataSource dataSource) {
    return builder
        .dataSource(dataSource)
        .packages("uk.ac.ebi.pride.archive.repo")
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