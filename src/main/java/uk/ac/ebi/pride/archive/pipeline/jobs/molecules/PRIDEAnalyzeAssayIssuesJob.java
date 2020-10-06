package uk.ac.ebi.pride.archive.pipeline.jobs.molecules;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveRedisConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.RepoConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.jobs.projects.PrideImportAssaysMongoJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.spectra.configs.AWS3Configuration;
import uk.ac.ebi.pride.mongodb.archive.repo.assay.PrideAssayMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.mongodb.configs.MoleculesMongoConfig;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Suresh Hewapathirana
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@ComponentScan({"uk.ac.ebi.pride.archive.pipeline.jobs.projects"})
@Import({RepoConfig.class, ArchiveMongoConfig.class, MoleculesMongoConfig.class,
        DataSourceConfiguration.class, AWS3Configuration.class, ArchiveRedisConfig.class})
public class PRIDEAnalyzeAssayIssuesJob extends AbstractArchiveJob {

    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideAssayMongoRepository assayMongoRepository;

    @Autowired
    PrideMoleculesMongoService moleculesService;

    @Autowired
    PrideImportAssaysMongoJob prideImportAssaysMongoJob;

    @Value("${pride.data.prod.directory}")
    String productionPath;

    @Value("${pride.data.backup.path}")
    String backupPath;

    @Autowired
    uk.ac.ebi.pride.archive.pipeline.services.redis.RedisMessageNotifier messageNotifier;

    @Value("${redis.assay.analyse.queue}")
    private String redisQueueName;


    @Bean
    public Job analyzeAssayIssueJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_ASSAY_ANALYSIS_ISSUES.getName())
                .start(analyzeAssayIssueStep())
                .build();
    }

    @Bean
    public Step analyzeAssayIssueStep() {
        return stepBuilderFactory
                .get("analyzeAssayIssuesStep")
                .tasklet((stepContribution, chunkContext) -> {

                    System.out.println("Molecule Statistics(" + Calendar.getInstance().getTime() );
                    System.out.println("=========================================================");

                    System.out.println("Number of Assays : " + assayMongoRepository.count());
                    System.out.println("Number of Protein Evidences : " + moleculesService.getNumberProteinEvidences());
                    System.out.println("Number of Peptide Evidences : " + moleculesService.getNumberPeptideEvidences());
                    System.out.println("Number of PSM Evidences : " + moleculesService.getNumberPSMEvidecnes());

                    System.out.println("------------------------------------------------------------------");

                    // find projects that have all empty files
                    Map<String,Long> projectSize = new HashMap<>();
                    for (Path directory : Files.newDirectoryStream(Paths.get(backupPath), path -> path.toFile().isDirectory())) {
                        Long totalFileSize = 0L;
                        if(directory.toFile().isDirectory()){
                            for (File file : Objects.requireNonNull(directory.toFile().listFiles())) {
                                if (file.isFile()){
                                    totalFileSize += file.length();
                                }
                            }
                        }
                        projectSize.put(directory.getFileName().toString(), totalFileSize);
                    }

                    Set<String> totallyFailedProjects = projectSize.entrySet()
                            .stream()
                            .filter(entry -> entry.getValue() == 0)
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());

                    System.out.println("Number of successful old projects : " + (projectSize.size() - totallyFailedProjects.size()));
                    System.out.println("Number of totally failed old projects : " + totallyFailedProjects.size());
                    System.out.println("Number of total old projects : " + projectSize.size());


                    Set<String> existingAssayAnalysisProjects = projectSize.keySet();

                    // find new projects that have not ran before
                    Set<String> publicProjects =   prideProjectMongoService.getAllProjectAccessions();
                    System.out.println("Number of public projects : " + publicProjects.size());
                    publicProjects.removeAll(existingAssayAnalysisProjects);
                    System.out.println("Number of public new projects : " + publicProjects.size());

                    for (String projectAccession :totallyFailedProjects) {
                        prideImportAssaysMongoJob.syncProject(projectAccession);
                    }

                    for (String projectAccession :publicProjects) {
                        prideImportAssaysMongoJob.syncProject(projectAccession);
                    }
                    return RepeatStatus.FINISHED;
                }).build();
    }

    public void reportErrorsFromLogs(String LOG_DIR) throws IOException {


        System.out.println("LOG_DIR : " + LOG_DIR);

        Map<String, Set<String>> logErrors = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();

        Set<String> fileList = listFilesUsingDirectoryStream(LOG_DIR);
        fileList.forEach(file -> {
            String LOG_FILE = LOG_DIR + "/" + file;
            Set<String> errors = checkLogsAndReport(LOG_FILE);
            logErrors.put(file, errors);
        });

        Map<String, Set<String>> logsByErrors = getLogsByError( logErrors);
        printLogsByErrors(logsByErrors);
        printErrorList(logsByErrors);
        logErrors.size();
    }

    public static Set<String> listFilesUsingDirectoryStream(String dir) throws IOException {
        Set<String> fileList = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path path : stream) {
                if (!Files.isDirectory(path)) {
                    if(path.getFileName().toString().endsWith(".log")){
                        fileList.add(path.getFileName().toString());
                    }
                }
            }
        }
        return fileList;
    }

    public Set<String> checkLogsAndReport(String filename){

        Set<String> errors = new HashSet<>();

        try {
            // input file
            FileInputStream fstream = new FileInputStream(filename);

            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String strLine;
            Boolean broken = false;
            int line = 0;
            StringBuilder stringBuilder = new StringBuilder();
            Set<String> errorsInJob = new HashSet<>();
            boolean isFailed = false;

            while ((strLine = br.readLine()) != null) {
                if (strLine.contains("Exception") || strLine.contains("ERROR")){
                    String errorMessage = getErrorType(strLine);
                    if(!errorMessage.equals("NO_ERROR")) {
                        errorsInJob.add(errorMessage);
                    }
                }
                if(strLine.contains("and the following status: [FAILED]")){
                    isFailed = true;
                }
            }
            if(isFailed){
                errors.addAll(errorsInJob);
            }

            in.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        return errors;
    }

    private String getErrorType(String strLine){
        String errorType = "NO_ERROR";

        if(strLine.contains("Could not get a valid score from PSM level!")){
            errorType = "Could not get a valid score from PSM level!";
        }else if(strLine.contains("Failed to flush writer")){
            errorType = "ERROR Failed to flush writer";
        }else if (strLine.contains("ERROR main uk.ac.ebi.pride.archive.pipeline.jobs.molecules.PRIDEAnalyzeAssayJob$2: No value present")){
            errorType = "No value present";
        }else if (strLine.contains("ERROR main uk.ac.ebi.pride.archive.pipeline.jobs.molecules.PRIDEAnalyzeAssayJob$2: null")){
            errorType = "PRIDEAnalyzeAssayJob$2: null";
        }else if (strLine.contains("808 ERROR main org.springframework.batch.core.step.AbstractStep: Encountered an error executing step PRIDE_ARCHIVE_MONGODB_ASSAY_INFERENCE in job analyzeAssayInformationJob")){
            errorType = "JmzReaderSpectrumService NULL";
        }else if (strLine.contains("ERROR main org.springframework.batch.core.step.AbstractStep: Encountered an error executing step PRIDE_ARCHIVE_MONGODB_ASSAY_INFERENCE in job analyzeAssayInformationJob")){
            errorType = "MzIdentMLFileParser.processModification: null";
        }else if (strLine.contains("Old modification which is changed:")){
            errorType = "Old modification which is changed";
        }else if(strLine.contains("File has") && strLine.contains("specIdLists")){
            errorType = "File has specIdLists";
        }else if(strLine.contains("MS2 query with index") && strLine.contains("does not exist in the MGF file")){
            errorType = "MS2 query with index does not exist in the MGF file";
        }
        return errorType;
    }

    private  Map<String, Set<String>> getLogsByError( Map<String, Set<String>> logErrors){

        Map<String, Set<String>> logsByErrors = new HashMap<>();

        logErrors.entrySet().forEach(stringSetEntry -> {
            if(stringSetEntry.getValue().size() !=0){
                for (String error: stringSetEntry.getValue()) {
                    if(logsByErrors.containsKey(error)){
                        Set<String> existingLogs = logsByErrors.get(error);
                        existingLogs.add(stringSetEntry.getKey());
                        logsByErrors.put(error, existingLogs);

                    }else{
                        logsByErrors.put(error, new HashSet<>(Arrays.asList(stringSetEntry.getKey())));
                    }
                }
            }else{
                if(logsByErrors.containsKey("No Errors")){
                    Set<String> existingLogs = logsByErrors.get("No Errors");
                    existingLogs.add(stringSetEntry.getKey());
                    logsByErrors.put("No Errors", existingLogs);

                }else{
                    logsByErrors.put("No Errors", new HashSet<>(Arrays.asList(stringSetEntry.getKey())));
                }
            }
        });
        return logsByErrors;
    }

    private void printLogsByErrors(Map<String, Set<String>> logsByErrors){
        logsByErrors.forEach((error, logs) -> {
            if(!error.equalsIgnoreCase("No Errors")) {
                System.out.println();
                System.out.println(error + " (" + logs.size() + ")");
                System.out.println("--------------------------------------------------------------");
                logs.forEach(log -> {
                    System.out.println("\t" + log);
                });
            }
        });
    }

    private void printErrorList(Map<String, Set<String>> logsByErrors){
        AtomicInteger count = new AtomicInteger();
        System.out.println("--------------------------------------------------------------");
        logsByErrors.forEach((errorName, logs) -> {
            count.addAndGet(logs.size());
            System.out.println(String.format("%-55s | %3s |",errorName, logs.size()));
        });
        System.out.println("--------------------------------------------------------------");
        System.out.println(String.format("%-55s | %3s |","TOTAL", count));
        System.out.println("==============================================================");
    }
}
