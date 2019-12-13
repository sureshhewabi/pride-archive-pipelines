package uk.ac.ebi.pride.archive.pipeline.utility;

import de.mpc.pia.intermediate.Modification;
import de.mpc.pia.modeller.psm.ReportPSM;
import uk.ac.ebi.jmzidml.model.mzidml.FileFormat;
import uk.ac.ebi.jmzidml.model.mzidml.SpectraData;
import uk.ac.ebi.pride.archive.spectra.utils.Constants;
import uk.ac.ebi.pride.utilities.util.Triple;

import java.nio.file.FileStore;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class contains a set of constants that are needed to process the data in the submission pipeline.
 *
 * @author ypriverol
 */
public class SubmissionPipelineConstants {

    /** Supported id format used in the spectrum file. */
    public enum SpecIdFormat {
        MASCOT_QUERY_NUM,
        MULTI_PEAK_LIST_NATIVE_ID,
        SINGLE_PEAK_LIST_NATIVE_ID,
        SCAN_NUMBER_NATIVE_ID,
        MZML_ID,
        MZDATA_ID,
        WIFF_NATIVE_ID,
        SPECTRUM_NATIVE_ID,
        WIFF_MGF_TITLE,
        NONE
    }

    private static final String SIGN = "[+-]";
    public static final String INTEGER = SIGN + "?\\d+";


    public enum FileType{
        PRIDE,
        MZTAB,
        MZID,
        MGF,
        MS2,
        MZML,
        MZXML,
        DTA,
        PKL,
        APL;

        public static FileType getFileTypeFromPRIDEFileName( String filename) {
            filename = returnUnCompressPath(filename.toLowerCase());
            if (filename.toLowerCase().endsWith("mzid") || filename.toLowerCase().endsWith("mzidentml")) {
                return MZID;
            } else if (filename.toLowerCase().endsWith("mzml")) {
                return MZML;
            } else if (filename.toLowerCase().endsWith("mgf")) {
                return MGF;
            } else if(filename.toLowerCase().endsWith("mzxml")) {
                return MZXML;
            }else if(filename.toLowerCase().endsWith("mztab")){
                return MZTAB;
            }else if(filename.toLowerCase().endsWith("apl")){
                return APL;
            }else if (filename.toLowerCase().endsWith(".xml"))
                return PRIDE;

            return null;
        }

        public static FileType getFileTypeFromSpectraData(SpectraData spectraData){
            FileFormat specFileFormat = spectraData.getFileFormat();
                if (specFileFormat != null) {
                    if (specFileFormat.getCvParam().getAccession().equals("MS:1000613")) return DTA;
                    if (specFileFormat.getCvParam().getAccession().equals("MS:1001062")) return MGF;
                    if (specFileFormat.getCvParam().getAccession().equals("MS:1000565")) return PKL;
                    if (specFileFormat.getCvParam().getAccession().equals("MS:1002996")) return APL;
                    if (specFileFormat.getCvParam().getAccession().equals("MS:1000584") || specFileFormat.getCvParam().getAccession().equals("MS:1000562"))
                        return MZML;
                    if (specFileFormat.getCvParam().getAccession().equals("MS:1000566")) return MZXML;
                    if (specFileFormat.getCvParam().getAccession().equals("MS:1001466")) return MS2;
                    if (specFileFormat.getCvParam().getAccession().equals("MS:1002600")) return PRIDE;
                }
                return null;
        }
    }

    public enum Compress_Type{
        GZIP("gz"),
        ZIP("zip");

        String extension;

        Compress_Type(String extension) {
            this.extension = extension;
        }

        public String getExtension() {
            return extension;
        }
    }

   public enum SubmissionsType{
        ALL, PUBLIC, PRIVATE
   }

  /**
   * NOTE: there are some external scripts calling individual jobs. If you are changing job
   * names, please change the those scripts as well
   * Eg:
   *    /nfs/pride/work/archive/revised-archive-submission-scripts/
   */
  public enum PrideArchiveJobNames {
       PRIDE_ARCHIVE_SOLR_MASTER_INIT("createPrideArchiveSolrCloudCollectionJob",
               "This command will create a new Collection of PRIDE Archive in SolrCloud Production."),

       PRIDE_ARCHIVE_ORACLE_MONGODB_SYNC("syncOracleToMongoProjectsJob",
               "This command will sync the Oracle Database data into MongoDB data"),

       PRIDE_ARCHIVE_MONGODB_SOLRCLOUD_SYNC("syncMongoProjectToSolrCloudJob",
               "This command sync all the projects from MongoDB to Solr Cloud"),

       PRIDE_ARCHIVE_RESET_SUBMISSION_MONGODB("resetMongoProjectsJob",
               "This command will reset the submission data from MongoDB"),

       PRIDE_ARCHIVE_RESET_SUBMISSION_SOLR("resetSolrProjectsJob",
               "This command will reset the submission data from Solr"),

       PRIDE_ARCHIVE_MONGODB_ANNOTATE_PROJECTS_COUNTRY("annotateProjectsWithCountryJob",
               "This job take a configuration file from github and annotate the Projects with the Country"),

       PRIDE_ARCHIVE_SUBMISSION_STATS("computeSubmissionStatsJob",
               "This command compute/estimate the statistics for PRIDE Submissions"),

       PRIDE_ARCHIVE_MONGODB_ASSAY_SYNC("importProjectAssaysInformationJob",
               "This command sync the Assay information from Oracle to MongoDB"),

       PRIDE_ARCHIVE_MONGODB_ASSAY_ANALYSIS("analyzeAssayInformationJob",
               "This command analyze the information of an assay"),

      PRIDE_ARCHIVE_SOLR_INDEX_PEPTIDE_PROTEIN("solrIndexPeptideProteinJob",
              "This command indexes peptides & proteins to Solr"),

      PRIDE_ARCHIVE_SYNC_MISSING_PROJECTS_SOLR("syncMissingProjectsToSolrJob",
              "This command indexes peptides & proteins to Solr"),

      PRIDE_ANALYZE_ASSAY_FROM_BACKUP_FILES("prideAnalyzeAssayFromBackupFilesJob",
              "Restore data from backup files"),

      PRIDE_USERS_AAP_SYNC("PrideUsersAAPSync", "This job will sync the users from PRIDE to AAP"),

      PRIDE_ARCHIVE_DATA_USAGE("calculatePrideArchiveDataUsage", "This job will calculate and collate PRIDE Archive data usage"),

      PRIDE_ARCHIVE_SYNC_MISSING_PROJECTS_ORACLE_MONGODB("syncMissingProjectsOracleToMongoJob", "This job will sync missing projects from Oracle into MongoDB"),

      PRIDE_ARCHIVE_SYNC_MISSING_PROJECTS_ORACLE_PC("syncMissingProjectsOracleToPXJob", "This job will sync missing projects from Oracle into ProteomeXchange"),

      PRIDE_ARCHIVE_MONGODB_MOLECULE_STATS("moleculeStatsJob", "This job compute some basic statistics across the entire PRIDE Archive");

      String name;
       String message;

       PrideArchiveJobNames(String name, String message) {
           this.name = name;
           this.message = message;
       }

       public String getName() {
           return name;
       }

       public String getMessage() {
           return message;
       }

   }

    public enum PrideArchiveStepNames{

       /** PRIDE SolrCloud Creation Tasks **/

       PRIDE_ARCHIVE_SOLR_CLOUD_DELETE_COLLECTION("deletePrideArchiveCollectionSolrCloudStep",
               "This Step will delete the collection PRIDE Archive in SolrCloud Production."),

        PRIDE_ARCHIVE_SOLR_CLOUD_CREATE_COLLECTION("createPrideArchiveCollectionSolrCloudStep",
                "This Step will create the collection PRIDE Archive in SolrCloud Production."),

        PRIDE_ARCHIVE_SOLR_CLOUD_REFINE_COLLECTION("refineArchiveCollectionSolrCloudStep",
                "This Step will refine the collection PRIDE Archive in SolrCloud Production."),

        /** PRIDE Jobs and Steps to estimate the stats  */
        PRIDE_ARCHIVE_SUBMISSION_STATS_YEAR("estimateSubmissionByYearStep",
                "This Step will estimate the number of submissions per year"),

        PRIDE_ARCHIVE_SUBMISSION_STATS_MONTH("estimateSubmissionByMonthStep",
                "This Step will estimate the number of submissions per year"),

        PRIDE_ARCHIVE_SUBMISSION_STATS_INSTRUMENT("estimateInstrumentsCountStep",
                "This step computes the number of submissions per instrument"),

        PRIDE_ARCHIVE_SUBMISSION_STATS_ORGANISM("estimateOrganismCountStep",
                "This step computes the number of submissions per organism"),

        PRIDE_ARCHIVE_SUBMISSION_STATS_ORGANISM_PART("estimateOrganismPartCountStep",
                "This step computes the number of submissions per organism part"),

        PRIDE_ARCHIVE_SUBMISSION_STATS_DISEASES("estimateDiseasesCountStep",
                "This step computes the number of submissions per organism"),

        PRIDE_ARCHIVE_SUBMISSION_STATS_MODIFICATIONS("estimateModificationCountStep",
                "This step computes the number of submissions per modifications"),

        PRIDE_ARCHIVE_SYNC_FILES_TO_PROJECT_SOLR("syncFilesToSolrProjectStep",
                "This step sync all the files that belong to a project to solr project, for searching"),

        PRIDE_ARCHIVE_SUBMISSION_STATS_COUNTRY("estimateCountryCountStep",
                "Number of submissions per Country of origin"),

        PRIDE_ARCHIVE_SUBMISSION_STATS_CATEGORY("estimateSubmissionByCategoryStep",
                "Number of submissions per Category - Organism, Organism Part"),

        PRIDE_ARCHIVE_MONGODB_ANNOTATE_PROJECTS_COUNTRY("annotateProjectsWithCountryStep",
                "This job take a configuration file from github and annotate the Projects with the Country"),

        /*AAP user sync*/
        PRIDE_USERS_AAP_SYNC("PrideUsersAAPSyncStep",
                "This step will sync pride users into AAP DB"),

        /** PRIDE Data Sync from Oracle to MongoDB **/

        PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC("syncProjectMongoDBToSolrCloudStep",
                "This Step will sync the Oracle Database data into MongoDB data"),

        PRIDE_ARCHIVE_MISSING_PROJ_ORACLE_TO_MONGO_SYNC("syncMissingProjectOracleToMongoDB",
                "This Step will sync missing projects from Oracle into MongoDB"),

        PRIDE_ARCHIVE_MISSING_PROJ_ORACLE_TO_PC_SYNC("syncMissingProjectsOracleToPXStep",
                "This Step will sync missing projects from Oracle into ProteomeXchange"),

        PRIDE_ARCHIVE_ORACLE_CLEAN_SOLR("cleanSolrCloudStep",
                "Clean all the documents in SolrCloud Master"),

        PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC_FILES("syncFileInformationToMongoDBStep",
                "This Step will sync all the Files in the Oracle data into MongoDB data"),

        PRIDE_ARCHIVE_RESET_SUBMISSION_MONGO("resetProjectMongoDBStep",
                "This Step will reset the project data in MongoDB"),

        PRIDE_ARCHIVE_RESET_FILES_SUBMISSION_MONGO("resetFileInformationMongoDBStep",
                "This Step will reset the files data in MongoDB"),

        PRIDE_ARCHIVE_RESET_SUBMISSION_SOLR("resetProjectSolrStep",
                "This Step will reset the project data in Solr"),

        PRIDE_ARCHIVE_SYNC_ASSAY_FILE("PrideArchiveAnnotateFileToAssayStep",
                "This Step will important the file information for each Project to MongoDB"),

        PRIDE_ARCHIVE_SYNC_ASSAY_TO_MONGO("importProjectAssayInformationStep",
                "This Step will import the assay information from Oracle to MongoDB"),

        PRIDE_ARCHIVE_MONGODB_ASSAY_INFERENCE("analyzeAssayInformationStep",
                "This step performns the assay inference"),

        PRIDE_ARCHIVE_MONGODB_ASSAY_UPDATE("updateAssayInformationStep",
                "This step performs the assay information update"),

        PRIDE_ARCHIVE_MONGODB_SPECTRUM_UPDATE("indexSpectraStep",
                "This step read the spectrum information from the file and insert it into mongoDB and S3"),

        PRIDE_ARCHIVE_MONGODB_PROTEIN_UPDATE("proteinPeptideIndexStep",
                "This step update the protein and peptide information"),

        PRIDE_ARCHIVE_SOLR_INDEX_PEPTIDE_PROTEIN("solrIndexProteinPeptideIndexStep",
                                                     "This step update the protein and peptide information"),

        PRIDE_ARCHIVE_SYNC_MISSING_PROJECTS_SOLR("solrSyncMissingProjectsStep",
                                                         "This step syncs missing projects to solr");

        String name;
        String message;

        PrideArchiveStepNames(String name, String message) {
            this.name = name;
            this.message = message;
        }

        public String getName() {
            return name;
        }

        public String getMessage() {
            return message;
        }

    }

    public static String buildInternalPath(String productionPath, String projectAccession, String publicationYear, String publicationMonth){
        return productionPath + publicationYear + "/" + publicationMonth + "/" + projectAccession + "/" + "internal/";
    }

    public static String returnUnCompressPath(String originalPath){
        if(originalPath.endsWith(Compress_Type.GZIP.extension) || originalPath.endsWith(Compress_Type.ZIP.extension)){
            return originalPath.substring(0, originalPath.length()-3);
        }
        return originalPath;
    }

    /**
     * Check if the ms File is supported and match with some of the par of the name in the Spectra Files
     * This method should be used in high-throughput, when you add different files.
     *
     * @param msIdentMLFiles List of  the MS files related with the MZIdentML
     * @return The relation between the SpectraData and the corresponding File.
     */
    public static List<Triple<String, SpectraData, SubmissionPipelineConstants.FileType>> combineSpectraControllers(String buildPath, List<String> msIdentMLFiles, List<SpectraData> spectraDataList) {

        List<Triple<String, SpectraData, SubmissionPipelineConstants.FileType>> spectraFileMap = new ArrayList<>();

        for (String file : msIdentMLFiles) {
            Iterator iterator = spectraDataList.iterator();
            while (iterator.hasNext()) {
                SpectraData spectraData = (SpectraData) iterator.next();
                if (spectraData.getLocation() != null && spectraData.getLocation().toLowerCase().contains(file.toLowerCase())) {
                    spectraFileMap.add(new Triple<>(buildPath + file, spectraData,
                            SubmissionPipelineConstants.FileType.getFileTypeFromSpectraData(spectraData)));
                }else if(file.contains(spectraData.getId())
                        || (spectraData.getName() != null && file.toLowerCase().contains(spectraData.getName().toLowerCase()))){
                    spectraFileMap.add(new Triple<>(buildPath + file, spectraData, SubmissionPipelineConstants
                            .FileType.getFileTypeFromSpectraData(spectraData)));
                }
            }
        }
        return spectraFileMap;
    }

    public static String getSpectrumId(uk.ac.ebi.jmzidml.model.mzidml.SpectraData spectraData, ReportPSM psm) {
        SpecIdFormat fileIdFormat = getSpectraDataIdFormat(spectraData.getSpectrumIDFormat().getCvParam().getAccession());


        if (fileIdFormat == SpecIdFormat.MASCOT_QUERY_NUM) {
            String rValueStr = psm.getSourceID().replaceAll("query=", "");
            String id = null;
            if(rValueStr.matches(INTEGER)){
                id = Integer.toString(Integer.parseInt(rValueStr) + 1);
            }
            return id;
        } else if (fileIdFormat == SpecIdFormat.MULTI_PEAK_LIST_NATIVE_ID) {
            String rValueStr = psm.getSourceID().replaceAll("index=", "");
            String id;
            if(rValueStr.matches(INTEGER)){
                id = Integer.toString(Integer.parseInt(rValueStr) + 1);
                return id;
            }
            return psm.getSourceID();
        } else if (fileIdFormat == SpecIdFormat.SINGLE_PEAK_LIST_NATIVE_ID) {
            return psm.getSourceID().replaceAll("file=", "");
        } else if (fileIdFormat == SpecIdFormat.MZML_ID) {
            return psm.getSourceID().replaceAll("mzMLid=", "");
        } else if (fileIdFormat == SpecIdFormat.SCAN_NUMBER_NATIVE_ID) {
            return psm.getSourceID().replaceAll("scan=", "");
        } else {
            return psm.getSpectrumTitle();
        }
    }

    public static String buildUsi(String projectAccession, Triple<String, SpectraData, FileType> refeFile, ReportPSM psm) {
        Constants.ScanType scanType = Constants.ScanType.INDEX;
        SpecIdFormat fileIFormat = getSpectraDataIdFormat(refeFile.getSecond().getSpectrumIDFormat().getCvParam().getAccession());
        String spectrumID = getSpectrumId(refeFile.getSecond(), psm);
        if(fileIFormat == SpecIdFormat.MASCOT_QUERY_NUM || fileIFormat == SpecIdFormat.MULTI_PEAK_LIST_NATIVE_ID){
            scanType = Constants.ScanType.INDEX;
        }else if(fileIFormat == SpecIdFormat.MZML_ID || fileIFormat == SpecIdFormat.SPECTRUM_NATIVE_ID){
            scanType = Constants.ScanType.SCAN;
            String[] scanStrings = spectrumID.split("scan=");
            spectrumID = scanStrings[1];
        }
        Path p = Paths.get(refeFile.getFirst());
        String fileName = p.getFileName().toString();
        return Constants.SPECTRUM_S3_HEADER + projectAccession + ":" + fileName + ":" + scanType.getName() + ":" + spectrumID + ":" + encodePSM(psm.getSequence(), psm.getModifications(), psm.getCharge());
    }

    /**
     * build USI for PRIDE XML as spectra
     * @param projectAccession project
     * @param fileName filename with the spectra
     * @param psm PSM
     * @return
     */
    public static String buildUsi(String projectAccession, String fileName,  ReportPSM psm) {
        Constants.ScanType scanType = Constants.ScanType.INDEX;
        return Constants.SPECTRUM_S3_HEADER + projectAccession + ":" + fileName + ":" + scanType.getName() + ":" + psm.getSourceID() + ":" + encodePSM(psm.getSequence(), psm.getModifications(), psm.getCharge());
    }

    public static String encodePSM(String sequence, Map<Integer, Modification> ptms, Integer charge) {
        return encodePeptide(sequence, ptms) + "/" + charge;
    }

    public static String encodePeptide(String sequence, Map<Integer, Modification> ptms){
        StringBuilder stringBuilder = new StringBuilder();
        String finalSequence = sequence;
        if(ptms != null && ptms.size() > 0){
            char[] sequenceList = sequence.toCharArray();
            if(ptms.containsKey(0))
                stringBuilder.append("[" + ptms.get(0).getAccession() + "]");
            for(int i = 0; i < sequenceList.length; i++){
                stringBuilder.append(sequenceList[i]);
                if(ptms.containsKey(i+1)){
                    stringBuilder.append("[" + ptms.get(i+1).getAccession() + "]");
                }
            }

            // Add the CTerm modifications
            for( Map.Entry entry: ptms.entrySet()){
                Integer position = (Integer) entry.getKey();
                Modification mod = (Modification) entry.getValue();
                if(position > sequence.length()){
                    stringBuilder.append("-").append("[").append(mod.getAccession()).append("]");
                }
            }
            finalSequence = stringBuilder.toString();
        }

        return finalSequence;

    }


    /**
     * Spectrum Id format for an specific CVterm accession
     *
     * @param accession CvTerm Accession
     * @return Specific Spectrum Id Format
     */
    public static SpecIdFormat getSpectraDataIdFormat(String accession) {
        if (accession.equals("MS:1001528")) return SpecIdFormat.MASCOT_QUERY_NUM;
        if (accession.equals("MS:1000774")) return SpecIdFormat.MULTI_PEAK_LIST_NATIVE_ID;
        if (accession.equals("MS:1000775")) return SpecIdFormat.SINGLE_PEAK_LIST_NATIVE_ID;
        if (accession.equals("MS:1001530")) return SpecIdFormat.MZML_ID;
        if (accession.equals("MS:1000776")) return SpecIdFormat.SCAN_NUMBER_NATIVE_ID;
        if (accession.equals("MS:1000770")) return SpecIdFormat.WIFF_NATIVE_ID;
        if (accession.equals("MS:1000777")) return SpecIdFormat.MZDATA_ID;
        if (accession.equals(("MS:1000768"))) return SpecIdFormat.SPECTRUM_NATIVE_ID;
        if (accession.equals("MS:1000796")) return SpecIdFormat.WIFF_MGF_TITLE;
        return SpecIdFormat.NONE;
    }
}
