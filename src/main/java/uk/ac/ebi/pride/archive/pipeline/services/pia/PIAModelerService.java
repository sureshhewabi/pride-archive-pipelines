package uk.ac.ebi.pride.archive.pipeline.services.pia;

import de.mpc.pia.intermediate.compiler.PIACompiler;
import de.mpc.pia.intermediate.compiler.PIASimpleCompiler;
import de.mpc.pia.intermediate.compiler.parser.InputFileParserFactory;
import de.mpc.pia.modeller.PIAModeller;
import de.mpc.pia.modeller.peptide.ReportPeptide;
import de.mpc.pia.modeller.protein.ReportProtein;
import de.mpc.pia.modeller.protein.inference.OccamsRazorInference;
import de.mpc.pia.modeller.protein.inference.SpectrumExtractorInference;
import de.mpc.pia.modeller.protein.scoring.AbstractScoring;
import de.mpc.pia.modeller.protein.scoring.MultiplicativeScoring;
import de.mpc.pia.modeller.protein.scoring.ProteinScoringFactory;
import de.mpc.pia.modeller.protein.scoring.settings.PSMForScoring;
import de.mpc.pia.modeller.psm.ReportPSM;
import de.mpc.pia.modeller.report.filter.AbstractFilter;
import de.mpc.pia.modeller.report.filter.FilterComparator;
import de.mpc.pia.modeller.report.filter.RegisteredFilters;
import de.mpc.pia.modeller.report.filter.impl.PSMScoreFilter;
import de.mpc.pia.modeller.score.FDRData;
import de.mpc.pia.modeller.score.ScoreModelEnum;
import lombok.extern.slf4j.Slf4j;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

        PIAModeller piaModeller = computeFDRPSMLevel(assayId, filePath, fileType);

        if (piaModeller != null){

            piaModeller.setCreatePSMSets(true);

            piaModeller.getPSMModeller().setAllDecoyPattern("searchengine");
            piaModeller.getPSMModeller().setAllTopIdentifications(0);

            piaModeller.getPSMModeller().calculateAllFDR();
            piaModeller.getPSMModeller().calculateCombinedFDRScore();

            piaModeller.setConsiderModifications(true);

            // protein level
            OccamsRazorInference seInference = new OccamsRazorInference();

            seInference.addFilter(
                    new PSMScoreFilter(FilterComparator.less_equal, false, 0.01, ScoreModelEnum.PSM_LEVEL_FDR_SCORE.getShortName()));

            seInference.setScoring(new MultiplicativeScoring(new HashMap<>()));
            seInference.getScoring().setSetting(AbstractScoring.SCORING_SETTING_ID, ScoreModelEnum.PSM_LEVEL_FDR_SCORE.getShortName());
            seInference.getScoring().setSetting(AbstractScoring.SCORING_SPECTRA_SETTING_ID, PSMForScoring.ONLY_BEST.getShortName());


            piaModeller.getProteinModeller().infereProteins(seInference);

            piaModeller.getProteinModeller().updateFDRData(FDRData.DecoyStrategy.SEARCHENGINE, "searchengine", 0.01);
            piaModeller.getProteinModeller().updateDecoyStates();
            piaModeller.getProteinModeller().calculateFDR();

        }

        return piaModeller;


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

            if (inferenceTempFile.exists()) {
                inferenceTempFile.deleteOnExit();
            }
        }
        return piaModeller;
    }

}
