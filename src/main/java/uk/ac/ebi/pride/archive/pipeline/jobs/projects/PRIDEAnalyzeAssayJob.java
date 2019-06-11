package uk.ac.ebi.pride.archive.pipeline.jobs.projects;


import de.mpc.pia.modeller.PIAModeller;
import de.mpc.pia.modeller.peptide.ReportPeptide;
import de.mpc.pia.modeller.protein.ReportProtein;
import de.mpc.pia.modeller.psm.ReportPSM;
import de.mpc.pia.modeller.report.filter.AbstractFilter;
import de.mpc.pia.modeller.report.filter.FilterComparator;
import de.mpc.pia.modeller.report.filter.impl.PeptideScoreFilter;
import de.mpc.pia.modeller.score.ScoreModelEnum;
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
import uk.ac.ebi.pride.mongodb.archive.model.param.MongoCvParam;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.repo.files.PrideFileMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.utilities.term.CvTermReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Configuration
@Slf4j
@Import({ArchiveMongoConfig.class, DataSourceConfiguration.class})
public class PRIDEAnalyzeAssayJob extends AbstractArchiveJob {

    private static final Long MERGE_FILE_ID = 0L;
    private static final Long FILE_ID = 1L;
    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoRepository prideFileMongoRepository;

    private PIAModelerService piaModellerService;

    private PIAModeller modeller;
    private MongoPrideAssay assay;

    @Bean
    PIAModelerService getPIAModellerService(){
        piaModellerService = new PIAModelerService();
        return piaModellerService;
    }

    @Value("${accession:#{null}}")
    private String projectAccession;

    @Value("${accession:#{null}}")
    private String assayAccession;

    @Value("${qValueThreshold:#{0.01}}")
    private Double qValueThershold;



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
                .next(updateAssayInformationStep())
                .build();
    }

    @Bean
    Step updateAssayInformationStep() {

        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_ASSAY_UPDATE.name())
                .tasklet((stepContribution, chunkContext) -> {

                    if(modeller != null && assay != null){

                        long nrDecoys = modeller.getPSMModeller().getReportPSMSets().entrySet().stream()
                                .filter(entry -> entry.getValue().getIsDecoy())
                                .count();

                        List<ReportPeptide> peptides = new ArrayList<>();
                        List<ReportProtein> proteins = new ArrayList<>();
                        List<ReportPSM> psms = new ArrayList<>();
                        if (nrDecoys > 0){
                            // setting filter for peptide level filtering
                            List<AbstractFilter> filters = new ArrayList<>();
                            filters.add(new PeptideScoreFilter(FilterComparator.less_equal, false, qValueThershold,
                                    ScoreModelEnum.PEPTIDE_LEVEL_Q_VALUE.getShortName()));              // you can also use fdr score here

                            // get the FDR filtered peptides
                            peptides = modeller.getPeptideModeller().getFilteredReportPeptides(MERGE_FILE_ID, filters);
                            proteins = modeller.getProteinModeller().getFilteredReportProteins(filters);
                            psms = modeller.getPSMModeller().getFilteredReportPSMs(MERGE_FILE_ID, filters);

                        }else{
                            peptides = modeller.getPeptideModeller().getFilteredReportPeptides(MERGE_FILE_ID, new ArrayList<>());
                            proteins = modeller.getProteinModeller().getFilteredReportProteins(new ArrayList<>());
                            psms    = modeller.getPSMModeller().getFilteredReportPSMs(FILE_ID, new ArrayList<>());
                        }

                        List<ReportPeptide> modifiedPeptides = peptides.stream().filter(x -> x.getModifications().size() > 0).collect(Collectors.toList());;

                        //Update reported peptides
                        List<MongoCvParam> summaryResults = assay.getSummaryResults();
                        List<MongoCvParam> newValues = new ArrayList<>(summaryResults.size());
                        for(MongoCvParam param: summaryResults){
                            param = updateValueOfMongoParamter(param, CvTermReference.PRIDE_NUMBER_ID_PEPTIDES, peptides.size());
                            param = updateValueOfMongoParamter(param, CvTermReference.PRIDE_NUMBER_ID_PROTEINS, proteins.size());
                            param = updateValueOfMongoParamter(param, CvTermReference.PRIDE_NUMBER_ID_PSMS, psms.size());
                            param = updateValueOfMongoParamter(param, CvTermReference.PRIDE_NUMBER_MODIFIED_PEPTIDES, modifiedPeptides.size());
                            newValues.add(param);
                        }
                        assay.setSummaryResults(newValues);
                        prideProjectMongoService.updateAssay(assay);

                    }

                    return RepeatStatus.FINISHED;
                }).build();
    }

    private MongoCvParam updateValueOfMongoParamter(MongoCvParam param, CvTermReference cvTerm, Integer value){
        if(param.getAccession().equalsIgnoreCase(cvTerm.getAccession())){
            param.setValue(String.valueOf(value));
        }
        return param;
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
                        modeller = piaModellerService.performProteinInference(assayAccession, "/Users/yperez/work/ms_work/pride_data/" + assayResultFile.get().getFileName().substring(0,  assayResultFile.get().getFileName().length() -3),
                                PIAModelerService.FileType.PRIDE, qValueThershold);
                        this.assay = assay.get();
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

}
