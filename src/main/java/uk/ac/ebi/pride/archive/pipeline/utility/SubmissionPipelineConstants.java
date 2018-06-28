package uk.ac.ebi.pride.archive.pipeline.utility;

/**
 * This class contains a set of constants that are needed to process the data in the submission pipeline.
 *
 * @author ypriverol
 */
public class SubmissionPipelineConstants {

   public enum SubmissionsType{
        ALL, PUBLIC, PRIVATE
   }

   public enum PrideArchiveJobNames{
       PRIDE_ARCHIVE_SOLR_MASTER_INIT("PrideArchiveSolrCloudInit", "This command will create a new Collection of PRIDE Archive in SolrCloud Production."),
       PRIDE_ARCHIVE_ORACLE_MONGODB_SYNC("PrideArchiveOracleSyncToMongoDB", "This command will sync the Oracle Database data into MongoDB data"),
       PRIDE_ARCHIVE_MONGODB_SOLRCLOUD_SYNC("PrideArchiveMongoSyncSolrCloud", "This command sync all the projects from MongoDB to Solr Cloud"),
       PRIDE_ARCHIVE_SUBMISSION_STATS("PrideArchiveSubmissionStats", "This command compute/estimate the statistics for PRIDE Submissions");

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

        /** PRIDE Data Sync from Oracle to MongoDB **/
       PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC("PrideArchiveOracleToMongoDBSyncStep", "This Step will sync the Oracle Database data into MongoDB data"),
       PRIDE_ARCHIVE_ORACLE_CLEAN_SOLR("PrideArchiveSolrCloudStep","Clean all the documents in SolrCloud Master"),
       PRIDE_ARCHIVE_ORACLE_TO_MONGO_SYNC_FILES("PrideArchiveOracleToMongoDBFilesStep", "This Step will sync all the Files in the Oracle data into MongoDB data");



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
