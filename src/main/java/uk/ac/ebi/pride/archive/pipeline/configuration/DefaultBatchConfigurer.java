package uk.ac.ebi.pride.archive.pipeline.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Driver;

@Configuration
@EnableJpaRepositories(entityManagerFactoryRef = "h2EntityManagerFactory", transactionManagerRef = "h2TransactionManager")
@Slf4j
public class DefaultBatchConfigurer extends org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer {

    @Value("${spring.datasource.driver.class.name}")
    private String springDataSourceDriverClassName;

    @Value("${spring.datasource.url}")
    private String springDataSourceUrl;

    @Value("${spring.datasource.username}")
    private String springDataSourceUsername;

    @Value("${spring.datasource.password}")
    private String springDataSourcePassword;

    @Bean(name = "dataSourceH2")
    @Primary
    public DataSource batchDataSource() {
        return createDatasource(
                springDataSourceDriverClassName,
                springDataSourceUrl,
                springDataSourceUsername,
                springDataSourcePassword);
    }

    static DataSource createDatasource(String driverClassName, String url, String username, String password) {
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
