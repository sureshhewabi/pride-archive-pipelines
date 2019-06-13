package uk.ac.ebi.pride.archive.pipeline.jobs.projects;


import de.mpc.pia.modeller.PIAModeller;
import de.mpc.pia.modeller.peptide.ReportPeptide;
import de.mpc.pia.modeller.protein.ReportProtein;
import de.mpc.pia.modeller.psm.ReportPSM;
import de.mpc.pia.modeller.report.filter.AbstractFilter;
import de.mpc.pia.modeller.report.filter.FilterComparator;
import de.mpc.pia.modeller.report.filter.impl.PeptideScoreFilter;
import de.mpc.pia.modeller.score.ScoreModelEnum;
import de.mpc.pia.tools.OntologyConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.dataprovider.data.peptide.PSMProvider;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParamProvider;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.services.pia.JmzReaderSpectrumService;
import uk.ac.ebi.pride.archive.pipeline.services.pia.PIAModelerService;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.spectra.configs.AWS3Configuration;
import uk.ac.ebi.pride.archive.spectra.model.ArchivePSM;
import uk.ac.ebi.pride.archive.spectra.model.CvParam;
import uk.ac.ebi.pride.archive.spectra.services.S3SpectralArchive;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoAssayFile;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoPrideAssay;
import uk.ac.ebi.pride.mongodb.archive.model.param.MongoCvParam;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.repo.files.PrideFileMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.tools.jmzreader.JMzReaderException;
import uk.ac.ebi.pride.tools.jmzreader.model.Spectrum;
import uk.ac.ebi.pride.utilities.term.CvTermReference;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration
@Slf4j
@Import({ArchiveMongoConfig.class, DataSourceConfiguration.class, AWS3Configuration.class})
public class PRIDEAnalyzeAssayJob extends AbstractArchiveJob {

    private static final Long MERGE_FILE_ID = 1L;
    private static final Long FILE_ID = 1L;
    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoRepository prideFileMongoRepository;

    private PIAModelerService piaModellerService;

    @Autowired
    S3SpectralArchive spectralArchive;

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

    /** List of reported Peptides **/
    List<ReportPeptide> peptides;

    /** Reported Proteins **/
    List<ReportProtein> proteins;



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
                .next(indexSpectra())
                .build();
    }

    @Bean
    public Step indexSpectra() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_SPECTRUM_UPDATE.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if(modeller != null && assay != null && peptides.size() > 0){
                        Optional<MongoAssayFile> assayResultFile = assay.getAssayFiles()
                                .stream().filter(x -> x.getFileCategory()
                                        .getValue().equalsIgnoreCase("RESULT")).findFirst();

                        if(assayResultFile.isPresent() && assayResultFile.get().getRelatedFiles().size() == 0){
                            Map<String, SubmissionPipelineConstants.FileType> files = Collections
                                    .singletonMap("/Users/yperez/work/ms_work/pride_data/" + assayResultFile.get().getFileName().substring(0,  assayResultFile.get().getFileName().length() -3), SubmissionPipelineConstants.FileType.PRIDE);
                            JmzReaderSpectrumService service = JmzReaderSpectrumService.getInstance(files);
                            peptides.forEach(peptide -> {

                                peptide.getPSMs().forEach(psm -> {
                                    try {
                                        Spectrum spectrum = service.getSpectrum("/Users/yperez/work/ms_work/pride_data/" + assayResultFile.get().getFileName().substring(0, assayResultFile.get().getFileName().length() - 3),
                                                psm.getSourceID());
                                        log.info(spectrum.getId() + " " + String.valueOf(psm.getMassToCharge() - spectrum.getPrecursorMZ()));
                                        double[] masses = new double[spectrum.getPeakList().size()];
                                        double[] intensities = new double[spectrum.getPeakList().size()];
                                        int count = 0;
                                        for(Map.Entry entry: spectrum.getPeakList().entrySet()){
                                            masses[count] = (Double)entry.getKey();
                                            intensities[count] = (Double) entry.getValue();
                                            count++;
                                        }

                                        List<CvParam> properties = new ArrayList<>();

                                        for(ScoreModelEnum scoreModel: ScoreModelEnum.values()){
                                            Double scoreValue = psm.getScore(scoreModel.getShortName());
                                            if(scoreValue != null && !scoreValue.isNaN()){
                                                for(CvTermReference ref: CvTermReference.values()){
                                                    if (ref.getAccession().equalsIgnoreCase(scoreModel.getCvAccession()))
                                                        properties.add(new CvParam(ref.getCvLabel(), ref.getAccession(), ref.getName(), String.valueOf(scoreValue)));
                                                }
                                            }
                                        }

                                        properties.add(new CvParam("MS", OntologyConstants.PSM_LEVEL_QVALUE.getPsiAccession(), OntologyConstants.PSM_LEVEL_QVALUE.getPsiName(), String.valueOf(psm.getQValue())));
                                        properties.add(new CvParam("MS", OntologyConstants.PEPTIDE_LEVEL_QVALUE.getPsiAccession(), OntologyConstants.PEPTIDE_LEVEL_QVALUE.getPsiName(), String.valueOf(peptide.getQValue())));

                                        PSMProvider archivePSM = ArchivePSM
                                                .builder()
                                                .peptideSequence(psm.getSequence())
                                                .deltaMass(psm.getDeltaMass())
                                                .msLevel(spectrum.getMsLevel())
                                                .precursorCharge(spectrum.getPrecursorCharge())
                                                .masses(masses)
                                                .intensities(intensities)
                                                .properties(properties)
                                                .usi("mzspec:" + assay.getProjectAccessions().iterator().next() + assayResultFile.get().getFileName().substring(0, assayResultFile.get().getFileName().length() - 3) + ":indexNumber:" + psm.getSourceID())
                                                .build();

                                        spectralArchive.writePSM(archivePSM.getUsi(), archivePSM);


                                    } catch (JMzReaderException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                });
                            });
                        }
                    }
                    return RepeatStatus.FINISHED;
                }).build();
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

                        peptides = new ArrayList<>();
                        proteins = new ArrayList<>();
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

                        List<ReportPeptide> modifiedPeptides = peptides.stream().filter(x -> x.getModifications().size() > 0).collect(Collectors.toList());

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

                        List<Tuple<MongoCvParam, Integer>> modificationCount = modifiedPeptides.stream()
                                .flatMap(x -> x.getModifications().values().stream())
                                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                                .entrySet().stream()
                                .map( entry -> new Tuple<MongoCvParam, Integer>(new MongoCvParam(entry.getKey().getCvLabel(), entry.getKey().getAccession(), entry.getKey().getDescription(), String.valueOf(entry.getKey().getMass())), entry.getValue().intValue()))
                                .collect(Collectors.toList());

                        assay.setSummaryResults(newValues);
                        assay.setPtmsResults(modificationCount);
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
                                SubmissionPipelineConstants.FileType.PRIDE, qValueThershold);
                        this.assay = assay.get();
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

}
