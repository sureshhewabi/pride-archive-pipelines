package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.repo.client.AssayRepoClient;
import uk.ac.ebi.pride.archive.repo.client.FileRepoClient;
import uk.ac.ebi.pride.archive.repo.client.PrideRepoClientFactory;
import uk.ac.ebi.pride.archive.repo.client.ProjectRepoClient;

@Configuration
public class RepoConfig {

    private final PrideRepoClientFactory prideRepoClientFactory;

    public RepoConfig(@Value("${pride-repo.api.baseUrl}") String apiBaseUrl,
                      @Value("${pride-repo.api.keyName}") String apiKeyName,
                      @Value("${pride-repo.api.keyValue}") String apiKeyValue) {
        this.prideRepoClientFactory = new PrideRepoClientFactory(apiBaseUrl, apiKeyName, apiKeyValue);
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

}
