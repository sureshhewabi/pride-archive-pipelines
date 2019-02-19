package uk.ac.ebi.pride.archive.pipeline.jobs.users;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;

@Configuration
@Slf4j
@Import({ArchiveOracleConfig.class, DataSourceConfiguration.class})
public class UserAccountsAAPSyncJob extends AbstractArchiveJob {

    //using JDBC template as user changes not commited into archive-repo


    //get users from PRIDE with USER_AAP_REF as null

    //check if user exists in AAP with same pride email<==>AAP username and filter the list

    //sync remaining elements into AAP
    /*@Bean
    public Job importUserJob(Step step1) {
        return jobBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_USERS_AAP_SYNC.getName())
                .incrementer(new RunIdIncrementer())
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<User> writer) {
        return stepBuilderFactory.get("step1")
                .<User, User> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }*/

}
