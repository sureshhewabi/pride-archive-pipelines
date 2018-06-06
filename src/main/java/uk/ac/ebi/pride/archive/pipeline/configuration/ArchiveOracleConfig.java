package uk.ac.ebi.pride.archive.pipeline.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.hibernate5.HibernateExceptionTranslator;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = {"uk.ac.ebi.pride.archive.repo.repos"})
@ComponentScan(basePackages = "uk.ac.ebi.pride.archive.repo.services")
@Slf4j
public class ArchiveOracleConfig {

  @Bean(name = "dataSourceOracle")
  @ConfigurationProperties(prefix = "spring.datasource.oracle")
  public DataSource dataSource() {
    return DataSourceBuilder.create().build();
  }

  @Bean(name = "entityManagerFactory")
  public LocalContainerEntityManagerFactoryBean entityManagerFactory(EntityManagerFactoryBuilder builder, @Qualifier("dataSourceOracle") DataSource dataSource) {
    return builder
            .dataSource(dataSource)
            .packages("uk.ac.ebi.pride.archive.repo.repos", "uk.ac.ebi.pride.archive.repo.services")
            .build();
  }

  @Bean(name = "transactionManager")
  public JpaTransactionManager jpaTransactionManager(
          @Qualifier("entityManagerFactory") EntityManagerFactory entityManagerFactory) {
    return new JpaTransactionManager(entityManagerFactory);
  }

  @Bean
  public HibernateExceptionTranslator hibernateExceptionTranslator() {
    return new HibernateExceptionTranslator();
  }
}
