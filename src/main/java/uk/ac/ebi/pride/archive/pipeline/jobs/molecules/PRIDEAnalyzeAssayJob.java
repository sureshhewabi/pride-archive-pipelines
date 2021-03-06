package uk.ac.ebi.pride.archive.pipeline.jobs.molecules;


import de.mpc.pia.intermediate.Accession;
import de.mpc.pia.intermediate.AccessionOccurrence;
import de.mpc.pia.intermediate.Modification;
import de.mpc.pia.intermediate.PeptideSpectrumMatch;
import de.mpc.pia.modeller.PIAModeller;
import de.mpc.pia.modeller.peptide.ReportPeptide;
import de.mpc.pia.modeller.protein.ReportProtein;
import de.mpc.pia.modeller.psm.ReportPSM;
import de.mpc.pia.modeller.report.filter.AbstractFilter;
import de.mpc.pia.modeller.report.filter.FilterComparator;
import de.mpc.pia.modeller.report.filter.RegisteredFilters;
import de.mpc.pia.modeller.report.filter.impl.PSMScoreFilter;
import de.mpc.pia.modeller.score.ScoreModelEnum;
import de.mpc.pia.tools.pride.PRIDETools;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DuplicateKeyException;
import uk.ac.ebi.jmzidml.model.mzidml.AbstractParam;
import uk.ac.ebi.jmzidml.model.mzidml.SpectraData;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.dataprovider.data.peptide.PSMProvider;
import uk.ac.ebi.pride.archive.dataprovider.data.peptide.PeptideSpectrumOverview;
import uk.ac.ebi.pride.archive.dataprovider.data.ptm.IdentifiedModification;
import uk.ac.ebi.pride.archive.dataprovider.data.ptm.IdentifiedModificationProvider;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParam;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParamProvider;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.services.pia.JmzReaderSpectrumService;
import uk.ac.ebi.pride.archive.pipeline.services.pia.PIAModelerService;
import uk.ac.ebi.pride.archive.pipeline.utility.BackupUtil;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.spectra.configs.AWS3Configuration;
import uk.ac.ebi.pride.archive.spectra.model.ArchiveSpectrum;
import uk.ac.ebi.pride.archive.spectra.services.S3SpectralArchive;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoAssayFile;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoPrideAssay;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.repo.files.PrideFileMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;
import uk.ac.ebi.pride.mongodb.molecules.model.protein.PrideMongoProteinEvidence;
import uk.ac.ebi.pride.mongodb.molecules.model.psm.PrideMongoPsmSummaryEvidence;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;
import uk.ac.ebi.pride.tools.jmzreader.JMzReaderException;
import uk.ac.ebi.pride.tools.jmzreader.model.Spectrum;
import uk.ac.ebi.pride.utilities.term.CvTermReference;
import uk.ac.ebi.pride.utilities.util.MoleculeUtilities;
import uk.ac.ebi.pride.utilities.util.Triple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, MoleculesMongoConfig.class,
        DataSourceConfiguration.class, AWS3Configuration.class, SolrCloudMasterConfig.class})
public class PRIDEAnalyzeAssayJob extends AbstractArchiveJob {

    private static final Long MERGE_FILE_ID = 1L;
    private static final Long FILE_ID = 1L;
    private static final int PIPELINE_RETRY_LIMIT = 20;
    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideFileMongoRepository prideFileMongoRepository;

    private PIAModelerService piaModellerService;

    @Autowired
    S3SpectralArchive spectralArchive;

    @Autowired
    PrideMoleculesMongoService moleculesService;

    @Autowired
    SolrProjectService solrProjectService;

    Map<String, Long> taskTimeMap = new HashMap<>();

    @Value("${pride.data.prod.directory}")
    String productionPath;

    @Value("${pride.data.backup.path}")
    String backupPath;

    private PIAModeller modeller;
    private MongoPrideAssay assay;

    private boolean isValid;
    private Set<CvParam> validationMethods = new HashSet<>();


    @Bean
    PIAModelerService getPIAModellerService() {
        piaModellerService = new PIAModelerService();
        return piaModellerService;
    }

    //@Value("#{jobParameters['project']}")
    private String projectAccession;

    //@Value("#{jobParameters['assay']}")
    private String assayAccession;

    @Value("${qValueThreshold:#{0.01}}")
    private Double qValueThreshold;

    @Value("${qFilterProteinFDR:#{1.0}}")
    private Double qFilterProteinFDR;


    /**
     * List of reported Peptides
     **/
    List<ReportPeptide> highQualityPeptides;
    private List<ReportPeptide> allPeptides;


    /**
     * Reported Proteins
     **/
    List<ReportProtein> highQualityProteins;
    private List<ReportProtein> allProteins;


    Map<Long, List<PeptideSpectrumOverview>> peptideUsi = new HashMap<>();
    List<ReportPSM> highQualityPsms;
    private List<ReportPSM> allPsms;


    long nrDecoys = 0;

    String buildPath;

    DecimalFormat df = new DecimalFormat("###.#####");

    private BufferedWriter proteinEvidenceBufferedWriter;
    private BufferedWriter peptideEvidenceBufferedWriter;
    private BufferedWriter archiveSpectrumBufferedWriter;
    private BufferedWriter psmSummaryEvidenceBufferedWriter;

    @Bean
    @StepScope
    public Tasklet initJobPRIDEAnalyzeAssayJob(@Value("#{jobParameters['project']}") String projectAccession, @Value("#{jobParameters['assay']}") String assayAccession) {
        return (stepContribution, chunkContext) ->
        {
            this.projectAccession = projectAccession;
            this.assayAccession = assayAccession;
            System.out.println(String.format("==================>>>>>>> PRIDEAnalyzeAssayJob - Run the job for Project %s Assay %s", projectAccession, assayAccession));
            return RepeatStatus.FINISHED;
        };
    }

    private void createBackupFiles() throws IOException {
        createBackupDir();
        final String peptideEvidenceFileName = BackupUtil.getPrideMongoPeptideEvidenceFile(backupPath, projectAccession, assayAccession);
        peptideEvidenceBufferedWriter = new BufferedWriter(new FileWriter(peptideEvidenceFileName, false));

        final String proteinEvidenceFileName = BackupUtil.getPrideMongoProteinEvidenceFile(backupPath, projectAccession, assayAccession);
        proteinEvidenceBufferedWriter = new BufferedWriter(new FileWriter(proteinEvidenceFileName, false));

        final String archiveSpectrumFileName = BackupUtil.getArchiveSpectrumFile(backupPath, projectAccession, assayAccession);
        archiveSpectrumBufferedWriter = new BufferedWriter(new FileWriter(archiveSpectrumFileName, false));

        final String psmSummaryEvidenceFileName = BackupUtil.getPrideMongoPsmSummaryEvidenceFile(backupPath, projectAccession, assayAccession);
        psmSummaryEvidenceBufferedWriter = new BufferedWriter(new FileWriter(psmSummaryEvidenceFileName, false));
    }

    private void createBackupDir() throws AccessDeniedException {
        String path = backupPath;
        if (!path.endsWith(File.separator)) {
            path = backupPath + File.separator;
        }
        path = path + projectAccession;
        File file = new File(path);
        if (file.exists() && file.isDirectory()) {
            return;
        }
        boolean mkdirs = file.mkdirs();
        if (!mkdirs) {
            throw new AccessDeniedException("Failed to create Dir : " + backupPath);
        }
    }

    @Bean
    public Step analyzeAssayInformationStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_ASSAY_INFERENCE.name())
                .tasklet((stepContribution, chunkContext) -> {

                    long initAnalysisAssay = System.currentTimeMillis();

                    log.info("Analyzing project -- " + projectAccession + " and Assay -- " + assayAccession);
                    log.info("creating backup files");
                    createBackupFiles();
                    log.info("creating backup files: finished");

                    Optional<MongoPrideProject> project = prideProjectMongoService.findByAccession(projectAccession);
                    Optional<MongoPrideAssay> assay = prideProjectMongoService.findAssayByAccession(assayAccession);
                    if (assay.isPresent() && project.isPresent()) {
                        Optional<MongoAssayFile> assayResultFile = assay.get().getAssayFiles()
                                .stream().filter(x -> x.getFileCategory()
                                        .getValue().equalsIgnoreCase("RESULT"))
                                .findFirst();
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM");
                        String allDate = dateFormat.format(project.get().getPublicationDate());
                        String[] allDateString = allDate.split("-");
                        String year = null, month = null;

                        SubmissionPipelineConstants.FileType fileType = SubmissionPipelineConstants.FileType.getFileTypeFromPRIDEFileName(assayResultFile.get().getFileName());

                        if (allDateString.length == 2) {
                            year = allDateString[0];
                            month = allDateString[1];
                        }
                        if (year != null && month != null) {
                            buildPath = SubmissionPipelineConstants.buildInternalPath(productionPath,
                                    projectAccession, year, month);

                            /**
                             * The first threshold for modeller is not threshold at PSM and Protein level.
                             */
                            modeller = piaModellerService.performProteinInference(assayAccession,
                                    SubmissionPipelineConstants.returnUnCompressPath(buildPath + assayResultFile.get().getFileName()),
                                    fileType, 1.0, 1.0);
                            this.assay = assay.get();


                            nrDecoys = modeller.getPSMModeller().getReportPSMSets().entrySet().stream()
                                    .filter(entry -> entry.getValue().getIsDecoy())
                                    .count();

                            List<AbstractFilter> filters = new ArrayList<>();
                            filters.add(RegisteredFilters.PSM_SOURCE_ID_FILTER
                                    .newInstanceOf(FilterComparator.equal,"index=null" ,true));

                            allPsms = modeller.getPSMModeller()
                                    .getFilteredReportPSMs(FILE_ID, filters);
                            allPeptides = modeller.getPeptideModeller()
                                    .getFilteredReportPeptides(MERGE_FILE_ID, filters);
                            allProteins = modeller.getProteinModeller()
                                    .getFilteredReportProteins(filters);



                            // setting filter for peptide level filtering
                            modeller = piaModellerService.performFilteringInference(modeller, qValueThreshold, qFilterProteinFDR);
                            filters.add(new PSMScoreFilter(FilterComparator.less_equal, false,
                                    qValueThreshold, ScoreModelEnum.PSM_LEVEL_Q_VALUE.getShortName()));              // you can also use fdr score here

                            // get the FDR filtered highQualityPeptides
                            highQualityPsms = modeller.getPSMModeller()
                                    .getFilteredReportPSMs(MERGE_FILE_ID, filters);
                            highQualityPeptides = modeller.getPeptideModeller()
                                    .getFilteredReportPeptides(MERGE_FILE_ID, filters);
                            highQualityProteins = modeller.getProteinModeller()
                                    .getFilteredReportProteins(filters);


                            if (!(nrDecoys > 0 && highQualityProteins.size() > 0 && highQualityPeptides.size() > 0 && highQualityPsms.size() > 0 && highQualityPsms.size() >= highQualityPeptides.size())) {
                                highQualityPeptides = new ArrayList<>();
                                highQualityProteins = new ArrayList<>();
                                highQualityPsms = new ArrayList<>();
                            }

                        } else {
                            String errorMessage = "The Year and Month for Project Accession can't be found -- " + project.get().getAccession();
                            log.error(errorMessage);
                            throw new IOException(errorMessage);
                        }
                    }

                    taskTimeMap.put(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_ASSAY_INFERENCE.getName(),
                            System.currentTimeMillis() - initAnalysisAssay);

                    return RepeatStatus.FINISHED;
                }).build();
    }

    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job analyzeAssayInformationJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_ASSAY_ANALYSIS.getName())
                .start(stepBuilderFactory
                        .get("initJobPRIDEAnalyzeAssayJob")
                        .tasklet(initJobPRIDEAnalyzeAssayJob(null, null))
                        .build())
                .next(analyzeAssayInformationStep())
                .next(updateAssayInformationStep())
                .next(indexSpectraStep())
                .next(proteinPeptideIndexStep())
                .next(analyzeAssayPrintTraceStep())
                .build();
    }

    @Bean
    public Step analyzeAssayPrintTraceStep() {
        return stepBuilderFactory
                .get("analyzeAssayPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    proteinEvidenceBufferedWriter.close();
                    peptideEvidenceBufferedWriter.close();
                    archiveSpectrumBufferedWriter.close();
                    psmSummaryEvidenceBufferedWriter.close();
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step proteinPeptideIndexStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_PROTEIN_UPDATE.name())
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

                        long initInsertPeptides = System.currentTimeMillis();

                        Set<String> proteinIds = new HashSet<>();
                        Set<String> peptideSequences = new HashSet<>();

                        List<ReportPeptide> peptides;
                        List<ReportProtein> proteins;
                        List<ReportPSM> psms = new ArrayList<>();

                        if (highQualityPeptides.size() > 0 && highQualityPsms.size() > 0 && highQualityProteins.size() > 0) {
                            peptides = highQualityPeptides;
                            proteins = highQualityProteins;
                            psms = highQualityPsms;
                        } else {
                            peptides = allPeptides;
                            proteins = allProteins;
                            psms = allPsms;
                        }

                        List<ReportPeptide> finalPeptides = peptides;

                        for (ReportProtein protein : proteins) {
                            String proteinSequence = protein.getRepresentative().getDbSequence();
                            String proteinAccession = protein.getRepresentative().getAccession();
                            Set<String> proteinGroups = protein.getAccessions()
                                    .stream().map(Accession::getAccession)
                                    .collect(Collectors.toSet());

                            List<IdentifiedModificationProvider> proteinPTMs = new ArrayList<>(convertProteinModifications(
                                    proteinAccession, protein.getPeptides()));

                            log.info(String.valueOf(protein.getQValue()));

                            CvParam scoreParam = null;
                            Set<CvParam> attributes = new HashSet<>();

                            if (!Double.isFinite(protein.getQValue()) && !Double.isNaN(protein.getQValue())) {

                                String value = df.format(protein.getQValue());

                                scoreParam = new CvParam(CvTermReference.MS_PIA_PROTEIN_GROUP_QVALUE.getCvLabel(),
                                        CvTermReference.MS_PIA_PROTEIN_GROUP_QVALUE.getAccession(),
                                        CvTermReference.MS_PIA_PROTEIN_GROUP_QVALUE.getName(), value);
                                attributes.add(scoreParam);
                            }

                            if (protein.getScore() != null && !protein.getScore().isNaN()) {
                                String value = df.format(protein.getScore());
                                scoreParam = new CvParam(CvTermReference.MS_PIA_PROTEIN_SCORE.getCvLabel(),
                                        CvTermReference.MS_PIA_PROTEIN_SCORE.getAccession(),
                                        CvTermReference.MS_PIA_PROTEIN_SCORE.getName(), value);
                                attributes.add(scoreParam);
                            }

                            AtomicReference<CvParam> param = new AtomicReference<>(new CvParam(PRIDETools.PrideOntologyConstants
                                    .PRIDE_SUBMITTERS_THERSHOLD.getCvLabel(),
                                    PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getAccession(),
                                    PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getName(),
                                    Boolean.toString(false)));

                            AtomicBoolean submitterValid = new AtomicBoolean(false);
                            protein.getPeptides().stream().forEach( x -> {
                                x.getPeptide().getSpectra().stream().forEach( y-> {
                                    for (AbstractParam abstractParam: y.getParams()){
                                        if(abstractParam instanceof uk.ac.ebi.jmzidml.model.mzidml.CvParam){
                                            uk.ac.ebi.jmzidml.model.mzidml.CvParam cv = (uk.ac.ebi.jmzidml.model.mzidml.CvParam) abstractParam;
                                            if(cv.getAccession().equalsIgnoreCase("PRIDE:0000511") &&
                                                    cv.getValue().equalsIgnoreCase("true")){
                                                param.set(new CvParam(PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD
                                                        .getCvLabel(),
                                                        PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getAccession(),
                                                        PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getName(),
                                                        Boolean.toString(true)));
                                                submitterValid.set(true);
                                            }
                                        }
                                    }
                                });
                            });

                            attributes.add(param.get());
                            proteinIds.add(proteinAccession);
                            protein.getPeptides().forEach(x -> peptideSequences.add(x.getSequence()));

                            PrideMongoProteinEvidence proteinEvidence = PrideMongoProteinEvidence
                                    .builder()
                                    .reportedAccession(proteinAccession)
                                    .isDecoy(protein.getIsDecoy())
                                    .proteinGroupMembers(proteinGroups)
                                    .ptms(proteinPTMs)
                                    .projectAccession(projectAccession)
                                    .proteinSequence(proteinSequence)
                                    .bestSearchEngineScore(scoreParam)
                                    .additionalAttributes(attributes)
                                    .assayAccession(assay.getAccession())
                                    .isValid(isValid)
                                    .qualityEstimationMethods(validationMethods)
                                    .numberPeptides(protein.getPeptides().size())
                                    .numberPSMs(protein.getNrPSMs())
                                    .sequenceCoverage(protein.getCoverage(proteinAccession))
                                    .build();

                            if(isValid || submitterValid.get()){
                                try {
                                    BackupUtil.write(proteinEvidence, proteinEvidenceBufferedWriter);
                                    moleculesService.insertProteinEvidences(proteinEvidence);
                                } catch (DuplicateKeyException ex) {
                                    moleculesService.saveProteinEvidences(proteinEvidence);
                                    log.debug("The protein was already in the database -- " + proteinEvidence.getReportedAccession());
                                } catch (Exception e) {
                                    log.error(e.getMessage(), e);
                                    throw new Exception(e);
                                }
                                indexPeptideByProtein(protein, finalPeptides);
                            }
                        }

                        taskTimeMap.put("InsertPeptidesProteinsIntoMongoDB", System.currentTimeMillis() - initInsertPeptides);

                        return RepeatStatus.FINISHED;
                    }
                }).build();
    }

    /**
     * This method index all the highQualityPeptides that identified a protein into the mongoDB
     *
     * @param protein  Identified Protein
     * @param peptides Collection of identified highQualityPeptides in the experiment
     */
    private void indexPeptideByProtein(ReportProtein protein, List<ReportPeptide> peptides) throws Exception {

        for (ReportPeptide peptide : protein.getPeptides()) {
            Optional<ReportPeptide> firstPeptide = peptides.stream()
                    .filter(globalPeptide -> globalPeptide.getStringID().equalsIgnoreCase(peptide.getStringID()))
                    .findFirst();

            if (firstPeptide.isPresent()) {

                Set<CvParam> peptideAttributes = new HashSet<>();
                if (!Double.isInfinite(firstPeptide.get().getQValue()) && !Double.isNaN(firstPeptide.get().getQValue())) {

                    String value = df.format(firstPeptide.get().getQValue());

                    CvParam peptideScore = new CvParam(CvTermReference.MS_PIA_PEPTIDE_QVALUE
                            .getCvLabel(),
                            CvTermReference.MS_PIA_PEPTIDE_QVALUE.getAccession(),
                            CvTermReference.MS_PIA_PEPTIDE_QVALUE.getName(), value);
                    peptideAttributes.add(peptideScore);
                }


                if (!Double.isInfinite(firstPeptide.get().getScore("peptide_fdr_score"))
                        && !Double.isNaN(firstPeptide.get().getScore("peptide_fdr_score"))) {

                    String value = df.format(firstPeptide.get().getScore("peptide_fdr_score"));

                    CvParam peptideScore = new CvParam(CvTermReference.MS_PIA_PEPTIDE_FDR
                            .getCvLabel(),
                            CvTermReference.MS_PIA_PEPTIDE_FDR.getAccession(),
                            CvTermReference.MS_PIA_PEPTIDE_FDR.getName(), value);
                    peptideAttributes.add(peptideScore);
                }

                List<PeptideSpectrumOverview> usiList = peptideUsi.get(firstPeptide.get().getPeptide().getID());

                int startPosition = 0;
                int endPosition = 0;

                Optional<AccessionOccurrence> occurrence = firstPeptide.get().getPeptide().getAccessionOccurrences().stream()
                        .filter(x -> x.getAccession().getAccession().equalsIgnoreCase(protein.getRepresentative().getAccession()))
                        .findFirst();
                if (occurrence.isPresent()) {
                    startPosition = occurrence.get().getStart();
                    endPosition = occurrence.get().getEnd();
                } else {
                    log.info("Position of the corresponding peptide is not present -- " + protein.getRepresentative().getAccession());
                }

                AtomicReference<CvParam> param = new AtomicReference<>(new CvParam(PRIDETools.PrideOntologyConstants
                        .PRIDE_SUBMITTERS_THERSHOLD.getCvLabel(),
                        PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getAccession(),
                        PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getName(), Boolean.toString(false)));

                AtomicBoolean submitterValid = new AtomicBoolean(false);
                peptides.stream().forEach( x -> {
                    x.getPeptide().getSpectra().stream().forEach(y -> {
                    for (AbstractParam abstractParam: y.getParams()){
                        if( abstractParam instanceof uk.ac.ebi.jmzidml.model.mzidml.CvParam){
                            if(abstractParam instanceof uk.ac.ebi.jmzidml.model.mzidml.CvParam){
                                uk.ac.ebi.jmzidml.model.mzidml.CvParam cv = (uk.ac.ebi.jmzidml.model.mzidml.CvParam) abstractParam;
                                if(cv.getAccession().equalsIgnoreCase("PRIDE:0000511") && cv.getValue().equalsIgnoreCase("true")){
                                    param.set(new CvParam(PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getCvLabel(),
                                            PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getAccession(),
                                            PRIDETools.PrideOntologyConstants.PRIDE_SUBMITTERS_THERSHOLD.getName(),
                                            Boolean.toString(true)));
                                    submitterValid.set(true);
                                }
                            }
                        }
                    }
                    });
                });

                peptideAttributes.add(param.get());

                PrideMongoPeptideEvidence peptideEvidence = PrideMongoPeptideEvidence
                        .builder()
                        .assayAccession(assay.getAccession())
                        .proteinAccession(protein.getRepresentative().getAccession())
                        .isDecoy(firstPeptide.get().getIsDecoy())
                        .peptideAccession(SubmissionPipelineConstants
                                .encodePeptide(peptide.getSequence(), peptide.getModifications()))
                        .peptideSequence(peptide.getSequence())
                        .additionalAttributes(peptideAttributes)
                        .projectAccession(projectAccession)
                        .psmAccessions(usiList)
                        .startPosition(startPosition)
                        .endPosition(endPosition)
                        .missedCleavages(firstPeptide.get().getMissedCleavages())
                        .ptmList(convertPeptideModifications(firstPeptide.get().getModifications()))
                        .isValid(isValid)
                        .qualityEstimationMethods(validationMethods)
                        .build();

                if(isValid || submitterValid.get()){
                    try {
                        BackupUtil.write(peptideEvidence, peptideEvidenceBufferedWriter);
                        moleculesService.insertPeptideEvidence(peptideEvidence);
                    } catch (DuplicateKeyException ex) {
                        moleculesService.savePeptideEvidence(peptideEvidence);
                        log.debug("The peptide evidence was already in the database -- " + peptideEvidence.getPeptideAccession());
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        throw new Exception(e);
                    }
                }
            }
        }

    }

    /**
     * Convert Peptide Modifications from PIA modeller to PeptideEvidence modifications
     *
     * @param modifications Modifications Map
     * @return List if {@link IdentifiedModificationProvider}
     */
    private Collection<? extends IdentifiedModificationProvider> convertPeptideModifications(Map<Integer, Modification> modifications) {

        List<IdentifiedModification> ptms = new ArrayList<>();

        for (Map.Entry<Integer, Modification> ptmEntry : modifications.entrySet()) {
            Modification ptm = ptmEntry.getValue();
            Integer position = ptmEntry.getKey();
            Set<CvParam> probabilities = ptm.getProbability()
                    .stream().map(oldProbability -> new CvParam(oldProbability.getCvLabel(),
                            oldProbability.getAccession(),
                            oldProbability.getName(),
                            String.valueOf(oldProbability.getValue())))
                    .collect(Collectors.toSet());
            // ignore modifications that can't be processed correctly (can not be mapped to the protein)
            if (ptm.getAccession() == null) {
                continue;
            }

            Optional<IdentifiedModification> proteinExist = ptms.stream()
                    .filter(currentMod -> currentMod.getModificationCvTerm()
                            .getAccession().equalsIgnoreCase(ptm.getAccession()))
                    .findAny();
            if (proteinExist.isPresent()) {
                proteinExist.get().addPosition(position, probabilities);
            } else {
                CvParam ptmName = new CvParam(ptm.getCvLabel(),
                        ptm.getAccession(), ptm.getDescription(),
                        String.valueOf(ptm.getMass()));
                IdentifiedModification newPTM = new IdentifiedModification(null, null, ptmName, null);
                newPTM.addPosition(position, probabilities);
                ptms.add(newPTM);
            }
        }
        return ptms;

    }

    /**
     * Convert peptide modifications to Protein modifications. Adjust the localization using the start and end positions.
     *
     * @param proteinAccession Protein Accession
     * @param peptides         List of highQualityPeptides
     * @return List of {@link IdentifiedModificationProvider}
     */
    private Collection<? extends IdentifiedModificationProvider> convertProteinModifications(String proteinAccession, List<ReportPeptide> peptides) {

        List<IdentifiedModification> ptms = new ArrayList<>();

        for (ReportPeptide item : peptides) {

            for (Map.Entry<Integer, Modification> ptmEntry : item.getModifications().entrySet()) {

                Modification ptm = ptmEntry.getValue();
                Integer position = ptmEntry.getKey();
                Set<CvParam> probabilities = ptm.getProbability()
                        .stream().map(oldProbability -> new CvParam(oldProbability.getCvLabel(),
                                oldProbability.getAccession(),
                                oldProbability.getName(),
                                String.valueOf(oldProbability.getValue())))
                        .collect(Collectors.toSet());
                // ignore modifications that can't be processed correctly (can not be mapped to the protein)
                if (ptm.getAccession() == null) {
                    continue;
                }

                // if we can calculate the position, we add it to the modification
                // -1 to calculate properly the modification offset
                item.getPeptide().getAccessionOccurrences().forEach(peptideEvidence -> {

                    if (peptideEvidence.getAccession().getAccession() == proteinAccession) {

                        if (peptideEvidence.getStart() != null && peptideEvidence.getStart() >= 0 && position >= 0) {

                            int startPos = peptideEvidence.getStart();
                            // n-term and c-term mods are not propagated to the protein except the case that the start
                            // position is 1 (beginning of the protein)
                            int proteinPosition = startPos + position - 1;

                            Optional<IdentifiedModification> proteinExist = ptms.stream()
                                    .filter(currentMod -> currentMod.getModificationCvTerm()
                                            .getAccession().equalsIgnoreCase(ptm.getAccession()))
                                    .findAny();
                            if (proteinExist.isPresent()) {
                                proteinExist.get().addPosition(proteinPosition, probabilities);
                            } else {
                                CvParam ptmName = new CvParam(ptm.getCvLabel(),
                                        ptm.getAccession(), ptm.getDescription(),
                                        String.valueOf(ptm.getMass()));
                                IdentifiedModification newPTM = new IdentifiedModification(null, null, ptmName, null);
                                newPTM.addPosition(proteinPosition, probabilities);
                                ptms.add(newPTM);
                            }

                            if (position > 0 && position < (item.getSequence().length() + 1)) {
//                                mod.addPosition(position, null);
//                                modifications.add(mod);
//                                log.info(String.valueOf(proteinPosition));
//                                log.info(ptm.getAccession());
                            } else if (position == 0) { //n-term for protein
//                                mod.addPosition(position, null);
//                                modifications.add(mod);
//                                log.info(String.valueOf(proteinPosition));
//                                log.info(ptm.getAccession());

                            }
                        } else {
//                            modifications.add(mod);
                            //if position is not set null is reported
                        }

                    }

                });

            }
        }
        return ptms;

    }

    @Bean
    public Step indexSpectraStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_SPECTRUM_UPDATE.name())
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        long initSpectraStep = System.currentTimeMillis();

                        List<ReportPeptide> peptides;
                        if (highQualityPeptides.size() > 0)
                            peptides = highQualityPeptides;
                        else
                            peptides = allPeptides;

                        if (modeller != null && assay != null && peptides.size() > 0) {

                            Optional<MongoAssayFile> assayResultFile = assay.getAssayFiles()
                                    .stream().filter(x -> x.getFileCategory()
                                            .getValue().equalsIgnoreCase("RESULT")).findFirst();

                            List<SpectraData> spectrumFiles = modeller.getSpectraData()
                                    .entrySet().stream().map(Map.Entry::getValue)
                                    .collect(Collectors.toList());

                            AtomicInteger totalPSM = new AtomicInteger();
                            AtomicInteger errorDeltaPSM = new AtomicInteger();

                            if (assayResultFile.isPresent() && (spectrumFiles.size() > 0
                                    || assayResultFile.get().getFileCategory().getAccession().equalsIgnoreCase("PRIDE:1002848"))) {

                                JmzReaderSpectrumService service = null;
                                List<Triple<String, SpectraData, SubmissionPipelineConstants.FileType>> mongoRelatedFiles = null;

                                if (spectrumFiles.size() > 0) {
                                    mongoRelatedFiles = (assayResultFile.get().getRelatedFiles().size() == 0) ?
                                            SubmissionPipelineConstants.combineSpectraControllers(buildPath,
                                                    Collections.singletonList(SubmissionPipelineConstants
                                                            .returnUnCompressPath(assayResultFile.get().getFileName())), spectrumFiles) :
                                            SubmissionPipelineConstants.combineSpectraControllers(buildPath, assayResultFile.get()
                                                    .getRelatedFiles().stream().map(x -> SubmissionPipelineConstants.returnUnCompressPath(x.getFileName()))
                                                    .collect(Collectors.toList()), spectrumFiles);

                                    service = JmzReaderSpectrumService.getInstance(mongoRelatedFiles);
                                } else {
                                    Triple<String, SpectraData, SubmissionPipelineConstants.FileType> prideSpectraFile = new Triple<>(SubmissionPipelineConstants.returnUnCompressPath(buildPath + assayResultFile.get().getFileName()), null, SubmissionPipelineConstants.FileType.PRIDE);
                                    service = JmzReaderSpectrumService.getInstance(Collections.singletonList(prideSpectraFile));
                                }

                                JmzReaderSpectrumService finalService = service;
                                List<Triple<String, SpectraData, SubmissionPipelineConstants.FileType>> finalMongoRelatedFiles = mongoRelatedFiles;

                                peptides.forEach(peptide -> peptide.getPSMs().forEach(psm -> {
                                    try {
                                        PeptideSpectrumMatch spectrum = null;
                                        if (psm instanceof ReportPSM)
                                            spectrum = ((ReportPSM) psm).getSpectrum();

                                        totalPSM.set(totalPSM.get() + 1);

                                        PeptideSpectrumMatch finalSpectrum = spectrum;
                                        System.out.println(finalSpectrum.getSourceID());

                                        Spectrum fileSpectrum = null;
                                        String spectrumFile = null;
                                        String fileName = null;
                                        Optional<Triple<String, SpectraData, SubmissionPipelineConstants.FileType>> refeFile = null;
                                        String usi = null;

                                        if (spectrumFiles.size() > 0) {
                                            refeFile = finalMongoRelatedFiles.stream()
                                                    .filter(x -> x.getSecond().getId()
                                                            .equalsIgnoreCase(finalSpectrum.getSpectrumIdentification()
                                                                    .getInputSpectra().get(0).getSpectraDataRef()))
                                                    .findFirst();
                                            spectrumFile = refeFile.get().getFirst();
                                            fileSpectrum = finalService.getSpectrum(spectrumFile,
                                                    SubmissionPipelineConstants.getSpectrumId(refeFile.get().getSecond(),
                                                            (ReportPSM) psm));
                                            usi = SubmissionPipelineConstants.buildUsi(projectAccession, refeFile.get(),
                                                    (ReportPSM) psm);
                                            Path p = Paths.get(refeFile.get().getFirst());
                                            fileName = p.getFileName().toString();
                                        } else {
                                            SpectraData spectraData = new SpectraData();
                                            spectraData.setLocation(assayResultFile.get().getFileName());
                                            refeFile = Optional.of(new Triple<>
                                                    (buildPath + assayResultFile.get().getFileName(),
                                                            spectraData, SubmissionPipelineConstants.FileType.PRIDE));
                                            fileSpectrum = finalService.getSpectrum(SubmissionPipelineConstants.returnUnCompressPath(buildPath + assayResultFile.get().getFileName()), ((ReportPSM) psm).getSourceID());
                                            usi = SubmissionPipelineConstants.buildUsi(projectAccession,
                                                    SubmissionPipelineConstants.returnUnCompressPath(assayResultFile.get().getFileName()), (ReportPSM) psm);
                                            spectrumFile = SubmissionPipelineConstants.returnUnCompressPath(assayResultFile.get().getFileName());
                                            fileName = SubmissionPipelineConstants.returnUnCompressPath(assayResultFile.get().getFileName());

                                        }

                                        log.info(fileSpectrum.getId() + " " + (psm.getMassToCharge() - fileSpectrum.getPrecursorMZ()));
                                        Double[] masses = new Double[fileSpectrum.getPeakList().size()];
                                        Double[] intensities = new Double[fileSpectrum.getPeakList().size()];
                                        int count = 0;
                                        for (Map.Entry entry : fileSpectrum.getPeakList().entrySet()) {
                                            masses[count] = (Double) entry.getKey();
                                            intensities[count] = (Double) entry.getValue();
                                            count++;
                                        }

                                        Set<CvParam> properties = new HashSet<>();
                                        Set<CvParam> psmAttributes = new HashSet<>();

                                        for (ScoreModelEnum scoreModel : ScoreModelEnum.values()) {
                                            Double scoreValue = psm.getScore(scoreModel.getShortName());
                                            if (scoreValue != null && !scoreValue.isNaN()) {
                                                for (CvTermReference ref : CvTermReference.values()) {
                                                    if (ref.getAccession().equalsIgnoreCase(scoreModel.getCvAccession())) {
                                                        CvParam cv = new CvParam(ref.getCvLabel(), ref.getAccession(),
                                                                ref.getName(), String.valueOf(scoreValue));
                                                        properties.add(cv);
                                                        if (ref.getAccession().equalsIgnoreCase("MS:1002355")) {
                                                            CvParam bestSearchEngine = new CvParam(cv.getCvLabel(), cv.getAccession(), cv.getName(), cv.getValue());
                                                            psmAttributes.add(bestSearchEngine);
                                                        }

                                                    }

                                                }
                                            }
                                        }

                                    // Capturing additional parameters provided by the user.
                                    boolean submitterValid = false;
                                    for(AbstractParam abstractParam: spectrum.getParams()){
                                        if(abstractParam != null){
                                            if(abstractParam instanceof uk.ac.ebi.jmzidml.model.mzidml.CvParam){
                                                uk.ac.ebi.jmzidml.model.mzidml.CvParam cvParam = (uk.ac.ebi.jmzidml.model.mzidml.CvParam) abstractParam;
                                                if(cvParam.getAccession() != null){
                                                    CvParam cv = new CvParam(cvParam.getCvRef(),
                                                            cvParam.getAccession(), cvParam.getName(), cvParam.getValue());
                                                    if(cv.getAccession().equalsIgnoreCase("PRIDE:0000511")){
                                                        psmAttributes.add(cv);
                                                        if(cv.getValue().equalsIgnoreCase("true"))
                                                            submitterValid = true;
                                                    }
                                                    properties.add(cv);
                                                }
                                            }
                                        }
                                    }
                                    properties.add(new CvParam(CvTermReference.MS_PIA_PEPTIDE_QVALUE.getCvLabel(),
                                            CvTermReference.MS_PIA_PEPTIDE_QVALUE.getAccession(),
                                            CvTermReference.MS_PIA_PEPTIDE_QVALUE.getName(),
                                            String.valueOf(peptide.getQValue())));

                                    double retentionTime = Double.NaN;
                                    if (psm.getRetentionTime() != null)
                                        retentionTime = psm.getRetentionTime();

                                    List<Double> ptmMasses = peptide.getModifications().entrySet()
                                            .stream().map(x -> x.getValue().getMass()).collect(Collectors.toList());
                                    double deltaMass = MoleculeUtilities
                                            .calculateDeltaMz(peptide.getSequence(),
                                                    spectrum.getMassToCharge(),
                                                    spectrum.getCharge(),
                                                    ptmMasses);

                                    log.info("Delta Mass -- " + deltaMass);

                                    if (deltaMass > 0.9) {
                                        errorDeltaPSM.set(errorDeltaPSM.get() + 1);
                                    }
                                    properties.add(new CvParam(CvTermReference.MS_DELTA_MASS.getCvLabel(),
                                            CvTermReference.MS_DELTA_MASS.getAccession(),
                                            CvTermReference.MS_DELTA_MASS.getName(),
                                            String.valueOf(deltaMass))
                                    );

                                    List<IdentifiedModification> mods = new ArrayList<>();
                                    if (psm.getModifications() != null && psm.getModifications().size() > 0)
                                        mods = PRIDEAnalyzeAssayJob.this.convertPeptideModifications(psm.getModifications()).stream().map(x -> {

                                                CvParam neutralLoss = null;
                                                if (x.getNeutralLoss() != null)
                                                    neutralLoss = new CvParam(x.getNeutralLoss().getCvLabel(),
                                                            x.getNeutralLoss().getAccession(),
                                                            x.getNeutralLoss().getName(), x.getNeutralLoss().getValue());

                                                List<Tuple<Integer, Set<? extends CvParamProvider>>> positionMap = new ArrayList<>();
                                                if (x.getPositionMap() != null && x.getPositionMap().size() > 0)
                                                    positionMap = x.getPositionMap().stream()
                                                            .map(y -> new Tuple<Integer, Set<? extends CvParamProvider>>(y.getKey(),
                                                                    y.getValue().stream()
                                                                            .map(z -> new CvParam(z.getCvLabel(),
                                                                                    z.getAccession(), z.getName(), z.getValue()))
                                                                    .collect(Collectors.toSet())))
                                                            .collect(Collectors.toList());

                                                CvParam modCv = null;
                                                if (x.getModificationCvTerm() != null)
                                                    modCv = new CvParam(x.getModificationCvTerm().getCvLabel(),
                                                            x.getModificationCvTerm().getAccession(),
                                                            x.getModificationCvTerm().getName(),
                                                            x.getModificationCvTerm().getValue());

                                                Set<CvParamProvider> modProperties = new HashSet<>();

                                                return new IdentifiedModification(neutralLoss, positionMap, modCv, modProperties);
                                            }).collect(Collectors.toList());

                                        PSMProvider archivePSM = ArchiveSpectrum
                                                .builder()
                                                .projectAccession(projectAccession)
                                                .assayAccession(assayAccession)
                                                .peptideSequence(psm.getSequence())
                                                .isDecoy(psm.getIsDecoy())
                                                .retentionTime(retentionTime)
                                                .msLevel(fileSpectrum.getMsLevel())
                                                .precursorCharge(fileSpectrum.getPrecursorCharge())
                                                .masses(masses)
                                                .numPeaks(intensities.length)
                                                .intensities(intensities)
                                                .properties(properties)
                                                .spectrumFile(spectrumFile)
                                                .modifications(mods)
                                                .precursorMz(fileSpectrum.getPrecursorMZ())
                                                .usi(usi)
                                                .spectrumFile(spectrumFile)
                                                .isValid(isValid)
                                                .missedCleavages(((ReportPSM) psm).getMissedCleavages())
                                                .qualityEstimationMethods(validationMethods.stream()
                                                        .map(x -> new CvParam(x.getCvLabel(),
                                                                x.getAccession(), x.getName(), x.getValue()))
                                                        .collect(Collectors.toSet()))
                                                .build();

                                        PrideMongoPsmSummaryEvidence psmMongo = PrideMongoPsmSummaryEvidence
                                                .builder()
                                                .usi(usi)
                                                .peptideSequence(psm.getSequence())
                                                .assayAccession(assayAccession)
                                                .isDecoy(psm.getIsDecoy())
                                                .charge(psm.getCharge())
                                                .isValid(isValid)
                                                .projectAccession(projectAccession)
                                                .fileName(fileName)
                                                .additionalAttributes(psmAttributes)
                                                .precursorMass(psm.getMassToCharge())
                                                .modifiedPeptideSequence(SubmissionPipelineConstants
                                                        .encodePeptide(psm.getSequence(), psm.getModifications()))
                                                .build();

                                        if(isValid || submitterValid ){
                                            try {
                                                BackupUtil.write(archivePSM, archiveSpectrumBufferedWriter);
                                                BackupUtil.write(psmMongo, psmSummaryEvidenceBufferedWriter);
                                                moleculesService.insertPsmSummaryEvidence(psmMongo);
                                            } catch (DuplicateKeyException ex) {
                                                moleculesService.savePsmSummaryEvidence(psmMongo);
                                                log.debug("The psm evidence was already in the database -- " + psmMongo.getUsi());
                                            }

                                            pushToS3(archivePSM.getUsi(), archivePSM, 0);

                                            List<PeptideSpectrumOverview> usis = new ArrayList<>();
                                            if (peptideUsi.containsKey(peptide.getPeptide().getID())) {
                                                usis = peptideUsi.get(peptide.getPeptide().getID());
                                            }
                                            usis.add(new PeptideSpectrumOverview(psm.getCharge(), psm.getMassToCharge(), usi));
                                            peptideUsi.put(peptide.getPeptide().getID(), usis);
                                        }

                                    }
                                    catch (Exception e) {
                                        log.error(e.getMessage(), e);
                                        if(!(e instanceof JMzReaderException))
                                            throw new RuntimeException(e);
                                    }
                                }));
                            }
                            log.info("Delta Mass Rate -- " + (errorDeltaPSM.get() / totalPSM.get()));
                        }

                        taskTimeMap.put(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_SPECTRUM_UPDATE.getName(),
                                System.currentTimeMillis() - initSpectraStep);

                        return RepeatStatus.FINISHED;
                    }
                }).build();
    }

    /**
     * This method allow to retry to push into S3 if the service is failing
     * @param usi usi to be push
     * @param psm PSM to be push
     * @param retry number of retries
     * @throws Exception
     */
    private void pushToS3(String usi, PSMProvider psm, int retry) throws Exception {
        try{
            spectralArchive.writePSM(usi, psm);
        }catch (com.amazonaws.SdkClientException e){
            Thread.sleep(10000);
            if(retry < PIPELINE_RETRY_LIMIT)
                pushToS3(usi, psm, retry + 1);
            else
                throw new Exception("The S3 is not working properly, multiple retries failed --");
        }
    }

    private String getSpectraLocation(SpectraData spectraData) {
        return null;
    }

    @Bean
    Step updateAssayInformationStep() {

        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_ASSAY_UPDATE.name())
                .tasklet((stepContribution, chunkContext) -> {

                    if (modeller != null && assay != null) {

                        List<ReportPeptide> modifiedPeptides = highQualityPeptides.
                                stream().filter(x -> x.getModifications().size() > 0)
                                .collect(Collectors.toList());

                        //Update reported highQualityPeptides
                        Set<CvParam> summaryResults = assay.getSummaryResults();
                        Set<CvParam> newValues = new HashSet<>(summaryResults.size());

                        for (CvParam param : summaryResults) {
                            param = updateValueOfMongoParamter(param,
                                    CvTermReference.PRIDE_NUMBER_ID_PEPTIDES, highQualityPeptides.size());
                            param = updateValueOfMongoParamter(param,
                                    CvTermReference.PRIDE_NUMBER_ID_PROTEINS, highQualityProteins.size());
                            param = updateValueOfMongoParamter(param,
                                    CvTermReference.PRIDE_NUMBER_ID_PSMS, highQualityPsms.size());
                            param = updateValueOfMongoParamter(param,
                                    CvTermReference.PRIDE_NUMBER_MODIFIED_PEPTIDES, modifiedPeptides.size());
                            newValues.add(param);
                        }

                        List<Tuple<CvParam, Integer>> modificationCount = modifiedPeptides.stream()
                                .flatMap(x -> x.getModifications().values().stream())
                                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                                .entrySet().stream()
                                .map(entry -> new Tuple<>(new CvParam(entry.getKey().getCvLabel(), entry.getKey().getAccession(), entry.getKey().getDescription(), String.valueOf(entry.getKey().getMass())), entry.getValue().intValue()))
                                .collect(Collectors.toList());


                        if (highQualityPeptides.size() > 0 && highQualityProteins.size() > 0 && highQualityPsms.size() > 0)
                            isValid = true;
                        else
                            isValid = false;

                        if (isValid) {
                            validationMethods.add(new CvParam(CvTermReference.MS_DECOY_VALIDATION_METHOD.getCvLabel(),
                                    CvTermReference.MS_DECOY_VALIDATION_METHOD.getAccession(), CvTermReference.MS_DECOY_VALIDATION_METHOD.getName(), String.valueOf(true)));
                        } else
                            validationMethods.add(new CvParam(CvTermReference.MS_DECOY_VALIDATION_METHOD.getCvLabel(),
                                    CvTermReference.MS_DECOY_VALIDATION_METHOD.getAccession(), CvTermReference.MS_DECOY_VALIDATION_METHOD.getName(), String.valueOf(false)));


                        assay.setSummaryResults(newValues);
                        assay.setIsValid(isValid);
                        assay.setQualityEstimationMethods(validationMethods);
                        assay.setPtmsResults(modificationCount);
                        // prideProjectMongoService.updateAssay(assay);

                    }

                    return RepeatStatus.FINISHED;
                }).build();
    }

    private CvParam updateValueOfMongoParamter(CvParam param, CvTermReference cvTerm, Integer value) {
        if (param.getAccession().equalsIgnoreCase(cvTerm.getAccession())) {
            param.setValue(String.valueOf(value));
        }
        return param;
    }


}
