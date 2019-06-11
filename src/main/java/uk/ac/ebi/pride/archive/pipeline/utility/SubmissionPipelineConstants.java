package uk.ac.ebi.pride.archive.pipeline.utility;

/**
 * This class contains a set of constants that are needed to process the data in the submission pipeline.
 *
 * @author ypriverol
 */
public class SubmissionPipelineConstants {

    public enum FileType{
        PRIDE,
        MZTAB,
        MZID,
        MGF,
        MS2,
        MZML,
        MZXML,
        APL
    }

   public enum SubmissionsType{
        ALL, PUBLIC, PRIVATE
   }

   public enum PrideArchiveJobNames{
       PRIDE_ARCHIVE_SOLR_MASTER_INIT("PrideArchiveSolrCloudInit", "This command will create a new Collection of PRIDE Archive in SolrCloud Production."),
       PRIDE_ARCHIVE_ORACLE_MONGODB_SYNC("PrideArchiveOracleSyncToMongoDB", "This command will sync the Oracle Database data into MongoDB data"),
       PRIDE_ARCHIVE_MONGODB_SOLRCLOUD_SYNC("PrideArchiveMongoSyncSolrCloud", "This command sync all the projects from MongoDB to Solr Cloud"),
       PRIDE_ARCHIVE_RESET_SUBMISSION_MONGODB("PrideArchiveResetSubmissionMongoDB", "This command will reset the submission data from MongoDB"),
       PRIDE_ARCHIVE_RESET_SUBMISSION_SOLR("PrideArchiveResetSubmissionSolr", "This command will reset the submission data from Solr"),
       PRIDE_ARCHIVE_MONGODB_ANNOTATE_PROJECTS_COUNTRY("PrideArchiveAnnotateProjectWithCountry", "This job take a configuration file from github and annotate the Projects with the Country"),
       PRIDE_ARCHIVE_SUBMISSION_STATS("PrideArchiveSubmissionStats", "This command compute/estimate the statistics for PRIDE Submissions"),
       PRIDE_ARCHIVE_MONGODB_ASSAY_SYNC("PrideImportAssayToMongo", "This command sync the Assay information from Oracle to MongoDB"),
       PRIDE_ARCHIVE_MONGODB_ASSAY_ANALYSIS("PrideAnalysisAssayInformation", "This command analyze the information of an assay"),

       PRIDE_USERS_AAP_SYNC("PrideUsersAAPSync", "This job will sync the users from PRIDE to AAP");

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

       PRIDE_ARCHIVE_SOLR_CLOUD_DELETE_COLLECTION("PrideArchiveSolrCloudDeleteCollectionStep", "This Step will delete the collection PRIDE Archive in SolrCloud Production."),
        PRIDE_ARCHIVE_SOLR_CLOUD_CREATE_COLLECTION("PrideArchiveSolrCloudCreateCollectionStep", "This Step will create the collection PRIDE Archive in SolrCloud Production."),
        PRIDE_ARCHIVE_SOLR_CLOUD_REFINE_COLLECTION("PrideArchiveSolrCloudRefineCollectionStep", "This Step will refine the collection PRIDE Archive in SolrCloud Production."),

        /** PRIDE Jobs and Steps to estimate the stats  */
        PRIDE_ARCHIVE_SUBMISSION_STATS_YEAR("PrideArchiveSubmissionStatsNumberByMonthStep", "This Step will estimate the number of submissions per year"),
        PRIDE_ARCHIVE_SUBMISSION_STATS_MONTH("PrideArchiveSubmissionStatsNumberByMonthStep", "This Step will estimate the number of submissions per year"),
        PRIDE_ARCHIVE_SUBMISSION_STATS_INSTRUMENT("PrideArchiveSubmissionStatsNumberByInstrumentsStep", "This step computes the number of submissions per instrument"),
        PRIDE_ARCHIVE_SUBMISSION_STATS_ORGANISM("PrideArchiveSubmissionStatsNumberByOrganismStep", "This step computes the number of submissions per organism"),
        PRIDE_ARCHIVE_SUBMISSION_STATS_ORGANISM_PART("PrideArchiveSubmissionStatsNumberByOrganismPartStep", "This step computes the number of submissions per organism part"),
        PRIDE_ARCHIVE_SUBMISSION_STATS_DISEASES("PrideArchiveSubmissionStatsNumberByDiseasesStep", "This step computes the number of submissions per organism"),
        PRIDE_ARCHIVE_SUBMISSION_STATS_MODIFICATIONS("PrideArchiveSubmissionStatsNumberByModificationsStep", "This step computes the number of submissions per modifications"),
        PRIDE_ARCHIVE_SYNC_FILES_TO_PROJECT_SOLR("PrideArchiveSyncProjectFromMongoToSolrProject", "This step sync all the files that belown to a project to solr project, for searching"),
        PRIDE_ARCHIVE_SUBMISSION_STATS_COUNTRY("PrideArchiveSubmissionStatsByCountry", "Number of submissions per Country of origin"),
        PRIDE_ARCHIVE_SUBMISSION_STATS_CATEGORY("PrideArchiveSubmissionStatsByCategory", "Number of submissions per Category - Organism, Organism Part"),

        PRIDE_ARCHIVE_MONGODB_ANNOTATE_PROJECTS_COUNTRY("PrideArchiveAnnotateProjectWithCountry", "This job take a configuration file from github and annotate the Projects with the Country"),


        /** PRIDE Data Sync from Oracle to MongoDB **/

        PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC("PrideArchiveOracleToMongoDBSyncStep", "This Step will sync the Oracle Database data into MongoDB data"),
        PRIDE_ARCHIVE_ORACLE_CLEAN_SOLR("PrideArchiveSolrCloudStep","Clean all the documents in SolrCloud Master"),
        PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC_FILES("PrideArchiveOracleToMongoDBFilesStep", "This Step will sync all the Files in the Oracle data into MongoDB data"),
        PRIDE_ARCHIVE_RESET_SUBMISSION_MONGO("PrideArchiveResetSubmissionMongoDBStep", "This Step will reset the project data in MongoDB"),
        PRIDE_ARCHIVE_RESET_FILES_SUBMISSION_MONGO("PrideArchiveResetFilesSubmissionMongoDBStep", "This Step will reset the files data in MongoDB"),
        PRIDE_ARCHIVE_RESET_SUBMISSION_SOLR("PrideArchiveResetSubmissionSolrStep", "This Step will reset the project data in Solr"),

        PRIDE_ARCHIVE_SYNC_ASSAY_FILE("PrideArchiveAnnotateFileToAssayStep", "This Step will important the file information for each Project to MongoDB"),
        PRIDE_ARCHIVE_SYNC_ASSAY_TO_MONGO("PrideArchiveImportAssayInformationStep", "This Step will import the assay information from Oracle to MongoDB"),
        PRIDE_ARCHIVE_MONGODB_ASSAY_INFERENCE("PrideAssayInference", "This step performns the assay inference"),
        PRIDE_ARCHIVE_MONGODB_ASSAY_UPDATE("PrideAssayUpdate", "This step performs the assay information update"),
        PRIDE_ARCHIVE_MONGODB_SPECTRUM_UPDATE("PrideAssaySpectrumUpdate", "This step read the spectrum information from the file and insert it into mongoDB and S3")
        ;

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
}
