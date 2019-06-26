package uk.ac.ebi.pride.archive.pipeline.jobs.projects;


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
import de.mpc.pia.modeller.report.filter.impl.PSMScoreFilter;
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
import uk.ac.ebi.jmzidml.model.mzidml.SpectraData;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.dataprovider.data.peptide.PSMProvider;
import uk.ac.ebi.pride.archive.dataprovider.data.ptm.DefaultIdentifiedModification;
import uk.ac.ebi.pride.archive.dataprovider.data.ptm.IdentifiedModificationProvider;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParamProvider;
import uk.ac.ebi.pride.archive.dataprovider.param.DefaultCvParam;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrCloudMasterConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.services.pia.JmzReaderSpectrumService;
import uk.ac.ebi.pride.archive.pipeline.services.pia.PIAModelerService;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.spectra.configs.AWS3Configuration;
import uk.ac.ebi.pride.archive.spectra.model.ArchiveSpectrum;
import uk.ac.ebi.pride.archive.spectra.model.CvParam;
import uk.ac.ebi.pride.archive.spectra.services.S3SpectralArchive;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoAssayFile;
import uk.ac.ebi.pride.mongodb.archive.model.assay.MongoPrideAssay;
import uk.ac.ebi.pride.mongodb.archive.model.param.MongoCvParam;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.repo.files.PrideFileMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PeptideSpectrumOverview;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;
import uk.ac.ebi.pride.mongodb.molecules.model.protein.PrideMongoProteinEvidence;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;
import uk.ac.ebi.pride.solr.indexes.pride.model.PrideSolrProject;
import uk.ac.ebi.pride.solr.indexes.pride.services.SolrProjectService;
import uk.ac.ebi.pride.tools.jmzreader.JMzReaderException;
import uk.ac.ebi.pride.tools.jmzreader.model.Spectrum;
import uk.ac.ebi.pride.utilities.term.CvTermReference;
import uk.ac.ebi.pride.utilities.util.MoleculeUtilities;
import uk.ac.ebi.pride.utilities.util.Triple;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration
@Slf4j
@Import({ArchiveMongoConfig.class, MoleculesMongoConfig.class,
        DataSourceConfiguration.class, AWS3Configuration.class, SolrCloudMasterConfig.class})
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

    @Autowired
    PrideMoleculesMongoService moleculesService;

    @Autowired
    SolrProjectService solrProjectService;

    @Value("${pride.data.prod.directory}")
    String productionPath;

    private PIAModeller modeller;
    private MongoPrideAssay assay;

    private boolean isValid;
    private List<MongoCvParam> validationMethods = new ArrayList<>();

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

    List<ReportPSM> psms;
    Map<Long, List<PeptideSpectrumOverview>> peptideUsi = new HashMap<>();

    long nrDecoys = 0;

    String buildPath;

    DecimalFormat df = new DecimalFormat("###.#####");


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
                .next(proteinPeptideIndex())
                .build();
    }

    @Bean
    public Step proteinPeptideIndex(){
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_SPECTRUM_UPDATE.name())
                .tasklet((stepContribution, chunkContext) -> {

                    Set<String> proteinIds = new HashSet<>();
                    Set<String> peptideSequences = new HashSet<>();

                    proteins.forEach(protein -> {

                        Set<String> proteinGroups = protein.getAccessions()
                                .stream().map(Accession::getAccession)
                                .collect(Collectors.toSet());

                        List<IdentifiedModificationProvider> proteinPTMs = new ArrayList<>();
                        proteinPTMs.addAll(convertProteinModifications(
                                protein.getRepresentative().getAccession(), protein.getPeptides()));

                        log.info(String.valueOf(protein.getQValue()));

                        DefaultCvParam scoreParam = null;
                        List<CvParamProvider> attributes = new ArrayList<>();


                        if(!Double.isFinite(protein.getQValue()) && !Double.isNaN(protein.getQValue())){

                            String value = df.format(protein.getQValue());

                            scoreParam = new DefaultCvParam(CvTermReference.MS_PIA_PROTEIN_GROUP_QVALUE.getCvLabel(),
                                    CvTermReference.MS_PIA_PROTEIN_GROUP_QVALUE.getAccession(),
                                    CvTermReference.MS_PIA_PROTEIN_GROUP_QVALUE.getName(), value);
                            attributes.add(scoreParam);
                        }

                        if(protein.getScore() != null && !protein.getScore().isNaN()){
                            String value = df.format(protein.getScore());
                            scoreParam = new DefaultCvParam(CvTermReference.MS_PIA_PROTEIN_SCORE.getCvLabel(),
                                    CvTermReference.MS_PIA_PROTEIN_SCORE.getAccession(),
                                    CvTermReference.MS_PIA_PROTEIN_SCORE.getName(), value);
                            attributes.add(scoreParam);
                        }

                        proteinIds.add(protein.getRepresentative().getAccession());
                        protein.getPeptides().stream().forEach(x -> peptideSequences.add(x.getSequence()));

                        PrideMongoProteinEvidence proteinEvidence = PrideMongoProteinEvidence
                                .builder()
                                .reportedAccession(protein.getRepresentative().getAccession())
                                .isDecoy(protein.getIsDecoy())
                                .proteinGroupMembers(proteinGroups)
                                .ptms(proteinPTMs)
                                .projectAccession(projectAccession)
                                .proteinSequence(protein.getRepresentative().getDbSequence())
                                .bestSearchEngineScore(scoreParam)
                                .additionalAttributes(attributes)
                                .assayAccession(assay.getAccession())
                                .isValid(isValid)
                                .qualityEstimationMethods(validationMethods)
                                .build();

                        moleculesService.saveProteinEvidences(proteinEvidence);

                        indexPeptideByProtein(protein, peptides);

                    });

                    PrideSolrProject solrProject = solrProjectService.findByAccession(projectAccession);
                    solrProject.addProteinIdentifications(proteinIds);
                    solrProject.addPeptideSequences(peptideSequences);
                    solrProjectService.update(solrProject);



                    return RepeatStatus.FINISHED;
                }).build();
    }

    /**
     * This method index all the peptides that identified a protein into the mongoDB
     * @param protein Identified Protein
     * @param peptides Collection of identified peptides in the experiment
     */
    private void indexPeptideByProtein(ReportProtein protein, List<ReportPeptide> peptides) {

        protein.getPeptides().forEach( peptide -> {

            Optional<ReportPeptide> firstPeptide = peptides.stream()
                    .filter(globalPeptide -> globalPeptide.getStringID().equalsIgnoreCase(peptide.getStringID()))
                    .findFirst();

            if(firstPeptide.isPresent()){

                List<CvParamProvider> peptideAttributes = new ArrayList<>();
                if(!Double.isInfinite(firstPeptide.get().getQValue()) && !Double.isNaN(firstPeptide.get().getQValue())){

                    String value = df.format(firstPeptide.get().getQValue());

                    CvParamProvider peptideScore = new DefaultCvParam(CvTermReference.MS_PIA_PEPTIDE_QVALUE
                            .getCvLabel(),
                            CvTermReference.MS_PIA_PEPTIDE_QVALUE.getAccession(),
                            CvTermReference.MS_PIA_PEPTIDE_QVALUE.getName(), value);
                    peptideAttributes.add(peptideScore);
                }


                if(!Double.isInfinite(firstPeptide.get().getScore("peptide_fdr_score"))
                        && !Double.isNaN(firstPeptide.get().getScore("peptide_fdr_score"))){

                    String value = df.format(firstPeptide.get().getScore("peptide_fdr_score"));

                    CvParamProvider peptideScore = new DefaultCvParam(CvTermReference.MS_PIA_PEPTIDE_FDR
                            .getCvLabel(),
                            CvTermReference.MS_PIA_PEPTIDE_FDR.getAccession(),
                            CvTermReference.MS_PIA_PEPTIDE_FDR.getName(), value);
                    peptideAttributes.add(peptideScore);
                }

                if(protein.getRepresentative().getAccession().equalsIgnoreCase("DECOY_ECA0723"))
                    System.out.println(protein.getRepresentative().getAccession());

                List<PeptideSpectrumOverview> usiList = peptideUsi.get(firstPeptide.get().getPeptide().getID());

                int startPosition = 0;
                int endPosition = 0;



                Optional<AccessionOccurrence> occurrence = firstPeptide.get().getPeptide().getAccessionOccurrences().stream()
                        .filter(x -> x.getAccession().getAccession().equalsIgnoreCase(protein.getRepresentative().getAccession()))
                        .findFirst();
                if(occurrence.isPresent()){
                    startPosition = occurrence.get().getStart();
                    endPosition = occurrence.get().getEnd();
                }else{
                    log.info("Position of the corresponding peptide is not present -- " + protein.getRepresentative().getAccession());
                }


                PrideMongoPeptideEvidence peptideEvidence = PrideMongoPeptideEvidence
                        .builder()
                        .assayAccession(assay.getAccession())
                        .proteinAccession(protein.getRepresentative().getAccession())
                        .isDecoy(firstPeptide.get().getIsDecoy())
                        .peptideAccession(peptide.getStringID())
                        .peptideSequence(peptide.getSequence())
                        .additionalAttributes(peptideAttributes)
                        .projectAccession(projectAccession)
                        .psmAccessions(usiList)
                        .startPosition(startPosition)
                        .endPosition(endPosition)
                        .missedCleavages(firstPeptide.get().getMissedCleavages())
                        .ptmList(convertPeptideModifications(firstPeptide.get()))
                        .isValid(isValid)
                        .qualityEstimationMethods(validationMethods)
                        .build();
                moleculesService.savePeptideEvidence(peptideEvidence);
            }


        });

    }

    /**
     * Convert Peptide Modifications from PIA modeller to PeptideEvidence modifications
     * @param peptide Peptide from PIA
     * @return List if {@link IdentifiedModificationProvider}
     */
    private Collection<? extends IdentifiedModificationProvider> convertPeptideModifications(ReportPeptide peptide) {

        List<DefaultIdentifiedModification> ptms = new ArrayList<>();

        for (Map.Entry<Integer, Modification> ptmEntry : peptide.getModifications().entrySet()) {
            Modification ptm = ptmEntry.getValue();
            Integer position = ptmEntry.getKey();
            List<DefaultCvParam> probabilities = ptm.getProbability()
                    .stream().map( oldProbability -> {
                        return  new DefaultCvParam(oldProbability.getCvLabel(),
                                oldProbability.getAccession(),
                                oldProbability.getName(),
                                String.valueOf(oldProbability.getValue()));
                    })
                    .collect(Collectors.toList());
            // ignore modifications that can't be processed correctly (can not be mapped to the protein)
            if (ptm.getAccession() == null) {
                continue;
            }


            Optional<DefaultIdentifiedModification> proteinExist = ptms.stream()
                    .filter(currentMod -> currentMod.getModificationCvTerm()
                            .getAccession().equalsIgnoreCase(ptm.getAccession()))
                    .findAny();
            if(proteinExist.isPresent()){
                proteinExist.get().addPosition(position, probabilities);
            }else{
                DefaultCvParam ptmName = new DefaultCvParam(ptm.getCvLabel(),
                        ptm.getAccession(), ptm.getDescription(),
                        String.valueOf(ptm.getMass()));
                DefaultIdentifiedModification newPTM = new DefaultIdentifiedModification(null, null, ptmName, null);
                newPTM.addPosition(position, probabilities);
                ptms.add(newPTM);
            }
        }
        return ptms;

    }

    /**
     * Convert peptide modifications to Protein modifications. Adjust the localization using the start and end positions.
     * @param proteinAccession Protein Accession
     * @param peptides List of peptides
     * @return List of {@link IdentifiedModificationProvider}
     */
    private Collection<? extends IdentifiedModificationProvider> convertProteinModifications(String proteinAccession, List<ReportPeptide> peptides) {

        List<DefaultIdentifiedModification> ptms = new ArrayList<>();

        for (ReportPeptide item : peptides) {

            for (Map.Entry<Integer, Modification> ptmEntry : item.getModifications().entrySet()) {

                Modification ptm = ptmEntry.getValue();
                Integer position = ptmEntry.getKey();
                List<DefaultCvParam> probabilities = ptm.getProbability()
                        .stream().map( oldProbability -> {
                            return  new DefaultCvParam(oldProbability.getCvLabel(),
                                    oldProbability.getAccession(),
                                    oldProbability.getName(),
                                    String.valueOf(oldProbability.getValue()));
                        })
                        .collect(Collectors.toList());
                // ignore modifications that can't be processed correctly (can not be mapped to the protein)
                if (ptm.getAccession() == null) {
                    continue;
                }

                // if we can calculate the position, we add it to the modification
                // -1 to calculate properly the modification offset
                item.getPeptide().getAccessionOccurrences().forEach(peptideEvidence ->{

                    if(peptideEvidence.getAccession().getAccession() == proteinAccession){

                        if (peptideEvidence.getStart() != null && peptideEvidence.getStart() >= 0 && position >= 0) {

                            int startPos = peptideEvidence.getStart();
                            // n-term and c-term mods are not propagated to the protein except the case that the start
                            // position is 1 (beginning of the protein)
                            int proteinPosition = startPos + position - 1;

                            Optional<DefaultIdentifiedModification> proteinExist = ptms.stream()
                                    .filter(currentMod -> currentMod.getModificationCvTerm()
                                            .getAccession().equalsIgnoreCase(ptm.getAccession()))
                                    .findAny();
                            if(proteinExist.isPresent()){
                                proteinExist.get().addPosition(proteinPosition, probabilities);
                            }else{
                                DefaultCvParam ptmName = new DefaultCvParam(ptm.getCvLabel(),
                                        ptm.getAccession(), ptm.getDescription(),
                                        String.valueOf(ptm.getMass()));
                                DefaultIdentifiedModification newPTM = new DefaultIdentifiedModification(null, null, ptmName, null);
                                newPTM.addPosition(proteinPosition, probabilities);
                                ptms.add(newPTM);
                            }

                            if (position > 0 && position < (item.getSequence().length() + 1)) {
//                                mod.addPosition(position, null);
//                                modifications.add(mod);
//                                log.info(String.valueOf(proteinPosition));
//                                log.info(ptm.getAccession());
                            } else if(position == 0) { //n-term for protein
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
    public Step indexSpectra() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_SPECTRUM_UPDATE.name())
                .tasklet((stepContribution, chunkContext) -> {
                    if(modeller != null && assay != null && peptides.size() > 0){
                        Optional<MongoAssayFile> assayResultFile = assay.getAssayFiles()
                                .stream().filter(x -> x.getFileCategory()
                                        .getValue().equalsIgnoreCase("RESULT")).findFirst();

                        List<SpectraData> spectrumFiles = modeller.getSpectraData()
                                .entrySet().stream().map(Map.Entry::getValue)
                                .collect(Collectors.toList());

                        AtomicInteger totalPSM = new AtomicInteger();
                        AtomicInteger errorDeltaPSM = new AtomicInteger();

                        if(assayResultFile.isPresent() && spectrumFiles.size() > 0)
                        {

                            List<Triple<String, SpectraData, SubmissionPipelineConstants.FileType>> mongoRelatedFiles = (assayResultFile.get().getRelatedFiles().size() == 0)?
                                    SubmissionPipelineConstants.combineSpectraControllers(buildPath,
                                            Collections.singletonList(SubmissionPipelineConstants
                                            .returnUnCompressPath(assayResultFile.get().getFileName())), spectrumFiles):
                                    SubmissionPipelineConstants.combineSpectraControllers(buildPath, assayResultFile.get()
                                            .getRelatedFiles().stream().map( x -> SubmissionPipelineConstants.returnUnCompressPath(x.getFileName()))
                                            .collect(Collectors.toList()), spectrumFiles);

                            JmzReaderSpectrumService service = JmzReaderSpectrumService.getInstance(mongoRelatedFiles);
                            peptides.parallelStream().forEach(peptide -> {

                                peptide.getPSMs().parallelStream().forEach(psm -> {
                                    try {
                                        PeptideSpectrumMatch spectrum = null;
                                        if(psm instanceof ReportPSM)
                                           spectrum = ((ReportPSM) psm).getSpectrum();

                                        totalPSM.set(totalPSM.get() + 1);

                                        PeptideSpectrumMatch finalSpectrum = spectrum;
                                        Optional<Triple<String, SpectraData, SubmissionPipelineConstants.FileType>> refeFile = mongoRelatedFiles.stream()
                                                .filter(x -> x.getSecond().getId()
                                                        .equalsIgnoreCase(finalSpectrum.getSpectrumIdentification()
                                                                .getInputSpectra().get(0).getSpectraDataRef()))
                                                .findFirst();

                                        String spectrumFile = refeFile.get().getFirst();
                                        Spectrum fileSpectrum = service.getSpectrum(spectrumFile, SubmissionPipelineConstants.getSpectrumId(refeFile.get().getSecond(), (ReportPSM) psm));
                                        log.info(fileSpectrum.getId() + " " + (psm.getMassToCharge() - fileSpectrum.getPrecursorMZ()));
                                        Double[] masses = new Double[fileSpectrum.getPeakList().size()];
                                        Double[] intensities = new Double[fileSpectrum.getPeakList().size()];
                                        int count = 0;
                                        for(Map.Entry entry: fileSpectrum.getPeakList().entrySet()){
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

//                                        properties.add(new CvParam(CvTermReference.MS_PIA_PSM_LEVEL_QVALUE.getCvLabel(),
//                                                CvTermReference.MS_PIA_PSM_LEVEL_QVALUE.getAccession(), CvTermReference.MS_PIA_PSM_LEVEL_QVALUE.getName(),
//                                                String.valueOf(psm.getQValue())));

                                        properties.add(new CvParam(CvTermReference.MS_PIA_PEPTIDE_QVALUE.getCvLabel(),
                                                CvTermReference.MS_PIA_PEPTIDE_QVALUE.getAccession(), CvTermReference.MS_PIA_PEPTIDE_QVALUE.getName(),
                                                String.valueOf(peptide.getQValue())));

                                        double retentionTime = Double.NaN;
                                        if(psm.getRetentionTime() != null)
                                            retentionTime = psm.getRetentionTime();

                                        List<Double> ptmMasses = peptide.getModifications().entrySet()
                                                .stream().map( x-> x.getValue().getMass()).collect(Collectors.toList());
                                        double deltaMass = MoleculeUtilities
                                                .calculateDeltaMz(peptide.getSequence(),
                                                        spectrum.getMassToCharge(),
                                                        spectrum.getCharge(),
                                                        ptmMasses);

                                        log.debug("Delta Mass -- " + deltaMass);

                                        if(deltaMass > 0.9){
                                           errorDeltaPSM.set(errorDeltaPSM.get() + 1);
                                        }

                                        properties.add(new CvParam(CvTermReference.MS_DELTA_MASS.getCvLabel(),
                                                CvTermReference.MS_DELTA_MASS.getAccession(),
                                                CvTermReference.MS_DELTA_MASS.getName(),
                                                String.valueOf(deltaMass))
                                        );

                                        PSMProvider archivePSM = ArchiveSpectrum
                                                .builder()
                                                .peptideSequence(psm.getSequence())
                                                .isDecoy(psm.getIsDecoy())
                                                .retentionTime(retentionTime)
                                                .msLevel(fileSpectrum.getMsLevel())
                                                .precursorCharge(fileSpectrum.getPrecursorCharge())
                                                .masses(masses)
                                                .intensities(intensities)
                                                .properties(properties)
                                                .spectrumFile(spectrumFile)
                                                .precursorMz(fileSpectrum.getPrecursorMZ())
                                                .usi(SubmissionPipelineConstants.buildUsi(
                                                        projectAccession,
                                                        refeFile.get(), (ReportPSM) psm))
                                                .isValid(isValid)
                                                .qualityEstimationMethods(validationMethods.stream()
                                                        .map( x-> new CvParam(x.getCvLabel(),
                                                                x.getAccession(), x.getName(), x.getValue()))
                                                        .collect(Collectors.toList()))
                                                .build();

                                        spectralArchive.writePSM(archivePSM.getUsi(), archivePSM);
                                        List<PeptideSpectrumOverview> usis = new ArrayList<>();
                                        if(peptideUsi.containsKey(peptide.getPeptide().getID())){
                                            usis = peptideUsi.get(peptide.getPeptide().getID());
                                        }
                                        usis.add(PeptideSpectrumOverview.builder()
                                                .usi(SubmissionPipelineConstants.buildUsi(
                                                        projectAccession,
                                                        refeFile.get(), (ReportPSM) psm))
                                                .charge(psm.getCharge())
                                                .precursorMass(psm.getMassToCharge())
                                                .build());
                                        peptideUsi.put(peptide.getPeptide().getID(), usis);

                                    } catch (JMzReaderException | IOException e) {
                                        e.printStackTrace();
                                    }
                                });
                            });
                        }
                        log.info("Delta Mass Rate -- " + (errorDeltaPSM.get() / totalPSM.get()));
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

    private String getSpectraLocation(SpectraData spectraData) {
        return null;
    }

    @Bean
    Step updateAssayInformationStep() {

        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_ASSAY_UPDATE.name())
                .tasklet((stepContribution, chunkContext) -> {

                    if(modeller != null && assay != null){

                        nrDecoys = modeller.getPSMModeller().getReportPSMSets().entrySet().stream()
                                .filter(entry -> entry.getValue().getIsDecoy())
                                .count();

                        peptides = new ArrayList<>();
                        proteins = new ArrayList<>();
                        if (nrDecoys > 0){
                            // setting filter for peptide level filtering
                            List<AbstractFilter> filters = new ArrayList<>();
                            filters.add(new PSMScoreFilter(FilterComparator.less_equal, false,
                                    qValueThershold,
                                    ScoreModelEnum.PSM_LEVEL_Q_VALUE.getShortName()));              // you can also use fdr score here

                            // get the FDR filtered peptides
                            peptides = modeller.getPeptideModeller().getFilteredReportPeptides(MERGE_FILE_ID, filters);
                            proteins = modeller.getProteinModeller().getFilteredReportProteins(filters);
                            psms = modeller.getPSMModeller().getFilteredReportPSMs(MERGE_FILE_ID, filters);

                        }else{
                            peptides = modeller.getPeptideModeller().getFilteredReportPeptides(MERGE_FILE_ID, new ArrayList<>());
                            proteins = modeller.getProteinModeller().getFilteredReportProteins(new ArrayList<>());
                            psms    = modeller.getPSMModeller().getFilteredReportPSMs(FILE_ID, new ArrayList<>());
                        }

                        List<ReportPeptide> modifiedPeptides = peptides.
                                stream().filter(x -> x.getModifications().size() > 0)
                                .collect(Collectors.toList());

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


                        isValid = false;
                        if(nrDecoys > 0){
                            validationMethods.add(new MongoCvParam(CvTermReference.MS_DECOY_VALIDATION_METHOD.getCvLabel(),
                                    CvTermReference.MS_DECOY_VALIDATION_METHOD.getAccession(), CvTermReference.MS_DECOY_VALIDATION_METHOD.getName(), String.valueOf(true)));
                            isValid = true;
                        }else
                            validationMethods.add(new MongoCvParam(CvTermReference.MS_DECOY_VALIDATION_METHOD.getCvLabel(),
                                    CvTermReference.MS_DECOY_VALIDATION_METHOD.getAccession(), CvTermReference.MS_DECOY_VALIDATION_METHOD.getName(), String.valueOf(false)));


                        assay.setSummaryResults(newValues);
                        assay.setIsValid(isValid);
                        assay.setQualityEstimationMethods(validationMethods);
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
                                        .getValue().equalsIgnoreCase("RESULT"))
                                .findFirst();
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM");
                        String allDate = dateFormat.format(project.get().getPublicationDate());
                        String[] allDateString = allDate.split("-");
                        String year = null, month = null;

                        SubmissionPipelineConstants.FileType fileType = SubmissionPipelineConstants.FileType.getFileTypeFromPRIDEFileName(assayResultFile.get().getFileName());

                        if(allDateString.length == 2){
                            year = allDateString[0];
                            month = allDateString[1];
                        }
                        if(year != null && month != null){
                            buildPath = SubmissionPipelineConstants.buildInternalPath(productionPath,
                                    projectAccession, year, month);
                            modeller = piaModellerService.performProteinInference(assayAccession,
                                    SubmissionPipelineConstants.returnUnCompressPath(buildPath + assayResultFile.get().getFileName()),
                                    fileType, qValueThershold, qValueThershold);
                            this.assay = assay.get();
                        }else{
                            String errorMessage = "The Year and Month for Project Accession can't be found -- " + project.get().getAccession();
                            log.error(errorMessage);
                            throw new IOException(errorMessage);
                        }
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

}
