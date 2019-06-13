package uk.ac.ebi.pride.archive.pipeline.services.pia;

import de.mpc.pia.intermediate.compiler.PIACompiler;
import de.mpc.pia.intermediate.compiler.PIASimpleCompiler;
import de.mpc.pia.intermediate.compiler.parser.InputFileParserFactory;
import de.mpc.pia.modeller.PIAModeller;
import de.mpc.pia.modeller.protein.inference.SpectrumExtractorInference;
import de.mpc.pia.modeller.protein.scoring.AbstractScoring;
import de.mpc.pia.modeller.protein.scoring.MultiplicativeScoring;
import de.mpc.pia.modeller.protein.scoring.settings.PSMForScoring;
import de.mpc.pia.modeller.report.filter.FilterComparator;
import de.mpc.pia.modeller.report.filter.impl.PSMScoreFilter;
import de.mpc.pia.modeller.score.ScoreModelEnum;
import lombok.extern.slf4j.Slf4j;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;

@Slf4j
public class PIAModelerService {

    private static final Long MERGE_FILE_ID = 1L;


    public PIAModelerService() {
    }

    /**
     * Perform the protein inference for a file. Including all the thershold.
     * @param filePath assay file path, pride xml, mzidentml
     * @param qThreshold q-value threshold
     */
    public PIAModeller performProteinInference(String assayId, String filePath, SubmissionPipelineConstants.FileType fileType, double qThreshold )
            throws IOException {

        PIAModeller modeller = computeFDRPSMLevel(assayId, filePath, fileType);

        if (modeller != null){
            long nrDecoys = modeller.getPSMModeller()
                    .getReportPSMSets().entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().getIsDecoy())
                    .count();

            if (modeller.getPSMModeller().getAllFilesHaveFDRCalculated()) {

                modeller.getPeptideModeller().calculateFDR(MERGE_FILE_ID);

                SpectrumExtractorInference seInference = new SpectrumExtractorInference();
                seInference.addFilter( new PSMScoreFilter(FilterComparator.less_equal,
                        false, qThreshold, ScoreModelEnum.PSM_LEVEL_FDR_SCORE.getShortName()));

                seInference.setScoring(new MultiplicativeScoring(new HashMap<>()));
                seInference.getScoring().setSetting(AbstractScoring.SCORING_SETTING_ID, ScoreModelEnum.PSM_LEVEL_FDR_SCORE.getShortName());
                seInference.getScoring().setSetting(AbstractScoring.SCORING_SPECTRA_SETTING_ID, PSMForScoring.ONLY_BEST.getShortName());

                modeller.getProteinModeller().infereProteins(seInference);
                modeller.getProteinModeller().updateDecoyStates();
                modeller.getProteinModeller().calculateFDR();

//                // get the FDR filtered peptides
//                List<ReportPeptide> peptides = modeller.getPeptideModeller()
//                        .getFilteredReportPeptides(MERGE_FILE_ID, new ArrayList<>());
//
//                List<ReportPeptide> noDecoyPeptides = new ArrayList<>();
//                if (!peptides.isEmpty()) {
//                    for (ReportPeptide peptide : peptides) {
//                        if (!peptide.getIsDecoy()) {
//                            noDecoyPeptides.add(peptide);
//                        }
//                    }
//                } else {
//                    //log.error("There are no peptides at all!");
//                }

                //log.info("number of FDR 0.01 filtered target peptides: " + noDecoyPeptides.size() + " / " + peptides.size());
            } else {
                log.info("No decoy information is present in the data!");
            }
        }

        return modeller;


    }

    /**
     * Compute the PSM FDR as PSM and Protein level
     * @param assayKey Assay Key
     * @param filePath File path of the assay
     * @param fileType File type, it can be mzTab, mzidentml or PRIDE xml
     * @return PIAModeller
     * @throws IOException
     */
    private PIAModeller computeFDRPSMLevel(String assayKey, String filePath, SubmissionPipelineConstants.FileType fileType) throws IOException {
        PIAModeller piaModeller = null;
        PIACompiler piaCompiler = new PIASimpleCompiler();

        String type = InputFileParserFactory.InputFileTypes.MZTAB_INPUT.getFileTypeShort();
        if(fileType == SubmissionPipelineConstants.FileType.PRIDE)
           type = InputFileParserFactory.InputFileTypes.PRIDEXML_INPUT.getFileTypeShort();
        else if (fileType == SubmissionPipelineConstants.FileType.MZID)
           type = InputFileParserFactory.InputFileTypes.MZIDENTML_INPUT.getFileTypeShort();

        piaCompiler.getDataFromFile(assayKey, filePath, null, type);

        piaCompiler.buildClusterList();
        piaCompiler.buildIntermediateStructure();


        if (piaCompiler.getAllPeptideSpectrumMatcheIDs() != null
                && !piaCompiler.getAllPeptideSpectrumMatcheIDs().isEmpty()) {

            File inferenceTempFile = File.createTempFile(assayKey, ".tmp");
            piaCompiler.writeOutXML(inferenceTempFile);
            piaCompiler.finish();
            piaModeller = new PIAModeller(inferenceTempFile.getAbsolutePath());
            piaModeller.setCreatePSMSets(true);
            piaModeller.getPSMModeller().setAllDecoyPattern("searchengine");
            piaModeller.getPSMModeller().setAllTopIdentifications(0);
            piaModeller.setConsiderModifications(true);

            // calculate FDR on PSM level
            piaModeller.getPSMModeller().calculateAllFDR();
            piaModeller.setConsiderModifications(false);
            piaModeller.getPSMModeller().updateDecoyStates(MERGE_FILE_ID);

            if (inferenceTempFile.exists()) {
                inferenceTempFile.deleteOnExit();
            }
        }
        return piaModeller;
    }

}
