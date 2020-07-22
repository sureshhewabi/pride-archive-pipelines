package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.repo.client.*;

@Configuration
public class RepoConfig {

    private final PrideRepoClientFactory prideRepoClientFactory;

    public RepoConfig(@Value("${pride-repo.api.baseUrl}") String apiBaseUrl,
                      @Value("${pride-repo.api.keyName}") String apiKeyName,
                      @Value("${pride-repo.api.keyValue}") String apiKeyValue,
                      @Value("${spring.application.name}") String appName) {

        this.prideRepoClientFactory = new PrideRepoClientFactory(apiBaseUrl, apiKeyName, apiKeyValue, appName);
    }

    @Bean
    public ProjectRepoClient getProjectRepoClient() {
        return prideRepoClientFactory.getProjectRepoClient();
    }

    @Bean
    public AssayRepoClient getAssayRepoClient() {
        return prideRepoClientFactory.getAssayRepoClient();
    }

    @Bean
    public FileRepoClient getFileRepoClient() {
        return prideRepoClientFactory.getFileRepoClient();
    }

    @Bean
    public CvParamRepoClient getCvParamRepoClient() {
        return prideRepoClientFactory.getCvParamRepoClient();

    }
    
    @Bean  
    public UserRepoClient getUserRepoClient() {
        return prideRepoClientFactory.getUserRepoClient();

    }

}
