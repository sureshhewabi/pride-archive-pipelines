package uk.ac.ebi.pride.archive.pipeline.jobs.projects;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.services.pia.PIAModelerService;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoAssayFile;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoPrideAssay;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.repo.files.PrideFileMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;

import java.util.Optional;

@Configuration
@Slf4j
@Import({ArchiveMongoConfig.class, DataSourceConfiguration.class})
public class PRIDEAnalyzeAssayJob extends AbstractArchiveJob {

    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoRepository prideFileMongoRepository;

    private PIAModelerService piaModellerService;

    @Bean
    PIAModelerService getPIAModellerService(){
        piaModellerService = new PIAModelerService();
        return piaModellerService;
    }

    @Value("${accession:#{null}}")
    private String projectAccession;

    @Value("${accession:#{null}}")
    private String assayAccession;

    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job analyzeAssayInformation() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_ASSAY_ANALYSIS.getName())
                .start(analyzeAssayInformationStep())
                .build();
    }

    @Bean
    Step analyzeAssayInformationStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_ASSAY_INFERENCE.name())
                .tasklet((stepContribution, chunkContext) -> {
                    Optional<MongoPrideProject> project = prideProjectMongoService.findByAccession(projectAccession);
                    Optional<MongoPrideAssay> assay = prideProjectMongoService.findAssayByAccession(assayAccession);
                    if(assay.isPresent() && project.isPresent()){
                        Optional<MongoAssayFile> assayResultFile = assay.get().getAssayFiles()
                                .stream().filter(x -> x.getFileCategory()
                                        .getValue().equalsIgnoreCase("RESULT")).findFirst();
                        piaModellerService.performProteinInference(assayAccession,
                                "/Users/yperez/work/ms_work/pride_data/" + assayResultFile.get().getFileName().substring(0,  assayResultFile.get().getFileName().length() -3),
                                PIAModelerService.FileType.PRIDE, 0.01);
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

}
