package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, SolrCloudMasterConfig.class, DataSourceConfiguration.class})
public class ResetProjectSolrJob extends AbstractArchiveJob {

    @Autowired
    SolrProjectService solrProjectService;

    @Value("${accession:#{null}}")
    private String accession;

    /**
     * This methods resets the data of a project from Mongo DB
     * @return
     */
    @Bean
    Step resetProjectSolr() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_RESET_SUBMISSION_SOLR.name())
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("############# job param accession:"+accession);
                    if(accession != null){
                        PrideSolrProject prideSolrProject = solrProjectService.findByAccession(accession);
                        if(prideSolrProject!=null) {
                            solrProjectService.deleteProjectById((String) prideSolrProject.getId());
                        }
                    }else{
                        throw new NullPointerException("Accession cannot be null");
                    }
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job syncOracleToMongoProjectsJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_RESET_SUBMISSION_SOLR.getName())
                .start(resetProjectSolr())
                .build();
    }

}
