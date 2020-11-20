package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.hateoas.Resource;
import uk.ac.ebi.biosamples.client.BioSamplesClient;
import uk.ac.ebi.biosamples.model.Attribute;
import uk.ac.ebi.biosamples.model.ExternalReference;
import uk.ac.ebi.biosamples.model.Relationship;
import uk.ac.ebi.biosamples.model.Sample;
import uk.ac.ebi.biosamples.model.SubmittedViaType;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.dataprovider.file.ProjectFileSource;
import uk.ac.ebi.pride.archive.dataprovider.file.ProjectFileType;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.HashUtils;
import uk.ac.ebi.pride.archive.pipeline.utility.PrideFilePathUtility;
import uk.ac.ebi.pride.archive.repo.client.FileRepoClient;
import uk.ac.ebi.pride.archive.repo.client.ProjectRepoClient;
import uk.ac.ebi.pride.archive.repo.models.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.models.project.Project;
import uk.ac.ebi.pride.data.exception.SubmissionFileException;
import uk.ac.ebi.pride.data.io.SubmissionFileParser;
import uk.ac.ebi.pride.data.io.SubmissionFileWriter;
import uk.ac.ebi.pride.data.model.DataFile;
import uk.ac.ebi.pride.data.model.Submission;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.model.sdrf.MongoPrideSdrf;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.sdrf.PrideSdrfMongoService;
import uk.ac.ebi.pride.solr.api.client.SolrProjectClient;
import uk.ac.ebi.pride.solr.commons.PrideSolrProject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.ac.ebi.pride.archive.pipeline.utility.PrideFilePathUtility.GENERATED;
import static uk.ac.ebi.pride.archive.pipeline.utility.PrideFilePathUtility.SUBMITTED;

/*

    This Job is get biosample id for samples and save to mongo for new sdrf files - post annotated

 */
@Configuration
@Slf4j
@EnableBatchProcessing
public class SaveSdrfFromGitHubToBioSamplesAndMongoJob extends AbstractArchiveJob {

    public static final String GITHUB_SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO_JOB = "githubSdrfSaveToBioSamplesAndMongoJob";
    public static final String READ_TSV_STEP = "readTsvStep";
    public static final String SAVE_TO_BIO_SAMPLES_AND_MONGO_STEP = "saveToBioSamplesAndMongoStep";
    public static final String PRIDE_DOMAIN = "self.pride";
    public static final String SOURCE_NAME = "source name";
    public static final String PRIDE_ARCHIVE_PROJECT_URL = "https://www.ebi.ac.uk/pride/archive/projects/";
    public static final String SAMPLE_CHECKSUM = "sampleChecksum";
    public static final String SAMPLE_ACCESSION = "sampleAccession";
    private static final String PULL_FROM_GITHUB = "pullFromGithub";
    private static final String SYNC_TO_FTP = "syncToFtp";
    private static final String SYNC_SCRIPT = "./syncProjectLDC.sh";
    public static final String BECOME_PRIDE_ADM_CP = "become pride_adm cp ";

    @Value("${pride.archive.data.path}")
    private String prideRepoRootPath;

    @Value(("${ftp.protocol.url}"))
    private String ftpProtocolUrl;

    @Value(("${ldc.staging.base.dir}"))
    private String ldcStagingBaseFolder;

    @Value("${projectsSdrfFolder:#{null}}")
    private String projectsSdrfFolder;

    @Value(("${github.folder.path}"))
    private String githubFolderPath;

    @Autowired
    private SolrProjectClient solrProjectClient;

    @Autowired
    private BioSamplesClient bioSamplesClient;

    @Autowired
    private PrideSdrfMongoService prideSdrfMongoService;

    private List<MongoPrideSdrf> samplesToSave = new ArrayList<>();

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    @Autowired
    private PrideFileMongoService prideFileMongoService;

    @Autowired
    private FileRepoClient fileRepoClient;

    @Autowired
    private ProjectRepoClient projectRepoClient;

    private Map<String, Map<String, Tuple<String, List<Record>>>> accessionToSdrfContents = new HashMap<>();

    @Value("${ftp.protocol.url}")
    private String ftpProtocol;

    @Value("${aspera.protocol.url}")
    private String asperaProtocol;

    @Bean
    public Job githubSdrfSaveToBioSamplesAndMongoJob() {
        return jobBuilderFactory
                .get(GITHUB_SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO_JOB)
                .start(pullFromGithub())
                .start(readTsvStep())
                .next(sdrfSaveToBioSamplesAndMongoStep())
                .next(syncSdrfFilesToMongo())
                .next(syncSdrfFilesToSolr())
                .next(syncToFtp())
                .build();
    }

    private Step pullFromGithub() {
        return stepBuilderFactory.get(PULL_FROM_GITHUB)
                .tasklet(pullFromGithubTasklet()).build();
    }

    private Tasklet pullFromGithubTasklet() {
        return ((stepContribution, chunkContext) -> {
            Git.open(new File(githubFolderPath))
                    .pull().call();
            return RepeatStatus.FINISHED;
        });
    }

    private Step readTsvStep() {
        return stepBuilderFactory
                .get(READ_TSV_STEP)
                .tasklet(readTsvTasklet()).build();
    }

    private Step sdrfSaveToBioSamplesAndMongoStep() {
        return stepBuilderFactory
                .get(SAVE_TO_BIO_SAMPLES_AND_MONGO_STEP)
                .tasklet(saveToBioSamplesAndMongoTasklet()).build();
    }

    private Step syncToFtp() {
        return stepBuilderFactory.get(SYNC_TO_FTP)
                .tasklet(syncToFtpTasklet()).build();
    }

    private Step syncSdrfFilesToMongo() {
        return stepBuilderFactory.get("syncSdrfFilesToMongo")
                .tasklet((stepContribution, chunkContext) -> {
                            List<String> accessions = accessionToSdrfContents.keySet().stream().collect(Collectors.toList());
                            prideProjectMongoService.findByMultipleAccessions(accessions).stream().forEach(this::doSdrfFileSync);
                            return RepeatStatus.FINISHED;
                        }
                ).build();
    }

    private Step syncSdrfFilesToSolr() {
        return stepBuilderFactory
                .get("syncSdrfFilesToSolr")
                .tasklet((stepContribution, chunkContext) -> {
                    List<String> accessions = accessionToSdrfContents.keySet().stream().collect(Collectors.toList());
                    prideProjectMongoService.findByMultipleAccessions(accessions).stream().forEach(this::doSolrSync);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }


    private void doSdrfFileSync(MongoPrideProject mongoPrideProject) {
        try {
            Project oracleProject = projectRepoClient.findByAccession(mongoPrideProject.getAccession());
            List<ProjectFile> oracleFiles = fileRepoClient.findAllByProjectId(oracleProject.getId());
            oracleFiles = oracleFiles.stream()
                    .filter(oracleFile -> oracleFile.getFileType().equals(ProjectFileType.EXPERIMENTAL_DESIGN))
                    .collect(Collectors.toList());

            if (oracleFiles == null || oracleFiles.size() == 0) {
                return;
            }
            List<Tuple<MongoPrideFile, MongoPrideFile>> status = prideFileMongoService.insertAllFilesAndMsRuns(PrideProjectTransformer.transformOracleFilesToMongoFiles(oracleFiles, null, oracleProject, ftpProtocol, asperaProtocol), null);
            log.info("Number of files has been inserted -- " + status.size());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private void doSolrSync(MongoPrideProject mongoPrideProject) {
        PrideSolrProject solrProject = PrideProjectTransformer.transformProjectMongoToSolr(mongoPrideProject);
        List<MongoPrideFile> files = prideFileMongoService.findFilesByProjectAccession(mongoPrideProject.getAccession());
        Set<String> fileNames = files.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
        solrProject.setProjectFileNames(fileNames);
        PrideSolrProject status = null;
        try {
            status = solrProjectClient.upsert(solrProject);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
        log.info("[Solr] The project -- " + status.getAccession() + " has been inserted in SolrCloud");
    }

    private Tasklet readTsvTasklet() {
        return (stepContribution, chunkContext) -> {
            TsvParserSettings tsvParserSettings = new TsvParserSettings();
            tsvParserSettings.setNullValue("not available");
            Files.walk(Paths.get(projectsSdrfFolder), Integer.MAX_VALUE).skip(1).forEach(file -> {
                String fileName = file.getFileName().toString();
                if (fileName.endsWith(".tsv")) {
                    String accession = file.getParent().getFileName().toString();
                    if(!prideProjectMongoService.findByAccession(accession).isPresent()){
                        return;
                    }
                    try {
                        String fileChecksum = HashUtils.getSha1Checksum(file.toFile());
                        if (!checkFilesAlreadySaved(fileChecksum, accession)) {
                            TsvParser tsvParser = new TsvParser(tsvParserSettings);
                            Map<String, Tuple<String, List<Record>>> sdrfContentsToProcess
                                    = accessionToSdrfContents.get(accession);
                            if (sdrfContentsToProcess == null) {
                                sdrfContentsToProcess = new HashMap<>();
                            }
                            sdrfContentsToProcess.put(fileChecksum, new Tuple(fileName,
                                    tsvParser.parseAllRecords(file.toFile())));
                            accessionToSdrfContents.put(accession, sdrfContentsToProcess);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Error in calculating checksum");
                    }
                }
            });
            return RepeatStatus.FINISHED;
        };
    }


    /*
     This method is to check whether the project is being republished with same sdrf.
     */
    private boolean checkFilesAlreadySaved(String fileChecksum, String projectAccession) {
        Set<String> fileChecksums = prideSdrfMongoService.getUniqueFileChecksumsOfProject(projectAccession);
        if (fileChecksums.contains(fileChecksum)) {
            return true;
        }
        return false;
    }

    private Tasklet saveToBioSamplesAndMongoTasklet() {
        return (stepContribution, chunkContext) -> {
            for (Map.Entry<String, Map<String, Tuple<String,List<Record>>>> accessionSdrfContent : accessionToSdrfContents.entrySet()) {
                String projectAccession = accessionSdrfContent.getKey();
                saveToBioSampleAndMongo(projectAccession, accessionSdrfContent.getValue());
            }
            return RepeatStatus.FINISHED;
        };
    }

    private void saveToBioSampleAndMongo(String accession, Map<String, Tuple<String,List<Record>>> fileCheckSumToSdrfContents) {
        Map<String, String> sampleChecksumAccession = getSampleChecksumAccession(accession);
        for (Map.Entry<String, Tuple<String,List<Record>>> fileCheckSumToSdrfContent : fileCheckSumToSdrfContents.entrySet()) {
            String fileChecksum = fileCheckSumToSdrfContent.getKey();
            List<Record> sdrfRecords = fileCheckSumToSdrfContent.getValue().getValue();
            String[] headers = sdrfRecords.get(0).getMetaData().headers();
            String fileName = fileCheckSumToSdrfContent.getValue().getKey();
            File outputSdrfFile = new File(getSdrfFilePath(accession) + fileName);
            TsvWriter tsvWriter = getTsvWriter(accession, outputSdrfFile, headers);
            sdrfRecords.remove(0); // remove header
            for (Record sdrfRecord : sdrfRecords) {
                String sampleName = accession + "-" + sdrfRecord.getString(SOURCE_NAME);
                Sample sample = Sample.build(sampleName,
                        null, PRIDE_DOMAIN, Instant.now(), null,
                        getAttributes(headers, sdrfRecord), getRelationShip(headers, sdrfRecord),
                        getExternalReferences(headers, sdrfRecord, accession), SubmittedViaType.JSON_API);

                // This is to check whether the sample already saved to biosamples
                String sampleChecksum = HashUtils.getSha256Checksum(sampleName + sample.getAttributes().toString());
                String sampleAccession = "";
                if (!sampleChecksumAccession.containsKey(sampleChecksum)) {
                    Resource<Sample> sampleResource = bioSamplesClient.persistSampleResource(sample);
                    sampleAccession = sampleResource.getContent().getAccession();
                    sampleChecksumAccession.put(sampleChecksum, sampleAccession);
                } else {
                    sampleAccession = sampleChecksumAccession.get(sampleChecksum);
                }
                Map<String, String> sampleToSave = createSample(headers, sdrfRecord, sampleAccession, sampleChecksum);
                sampleToSave.put(headers[0], sampleName);
                saveSamplesToMongo(accession, fileChecksum, sampleToSave);
                saveRowToFile(tsvWriter, sdrfRecord, sampleName,
                        sampleChecksum, sampleAccession, headers);
            }
            tsvWriter.close();
            addToSubmissionFileAndCopyToStaging(accession, outputSdrfFile);
            createFileInPostgres(outputSdrfFile, accession);
        }
        if (samplesToSave.size() > 0) {
            prideSdrfMongoService.saveSdrfList(samplesToSave);
        }
    }

    private void createFileInPostgres(File outputSdrfFile, String accession) {
        try {
            Project project = projectRepoClient.findByAccession(accession);
            ProjectFile projectFile = new ProjectFile();
            projectFile.setFileName(outputSdrfFile.getName());
            projectFile.setFileSize(Files.size(outputSdrfFile.toPath()));
            projectFile.setFileType(ProjectFileType.EXPERIMENTAL_DESIGN);
            projectFile.setProjectId(project.getId());
            projectFile.setFileSource(ProjectFileSource.SUBMITTED);
            fileRepoClient.save(projectFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addToSubmissionFileAndCopyToStaging(String accession, File file) {
        Optional<MongoPrideProject> mongoPrideProject = prideProjectMongoService.findByAccession(accession);
        String submissionFilePath = PrideFilePathUtility.getSubmissionFilePath(mongoPrideProject.get(), prideRepoRootPath);
        log.info("Processing submission file " + submissionFilePath);
        try {
            File submissionFile = new File(submissionFilePath);
            Submission submission = SubmissionFileParser.parse(submissionFile);
            DataFile sdrfDataFile = new DataFile(file, ProjectFileType.EXPERIMENTAL_DESIGN);
            addToSubmissonFile(submission, submissionFile, sdrfDataFile);
            addToReadMeFileAndCopyToStaging(mongoPrideProject.get(), sdrfDataFile);
        } catch (Exception ex) {
            log.error("Error in parsing Submission file for accession: " + accession);
        }

    }

    private void addToReadMeFileAndCopyToStaging(MongoPrideProject mongoPrideProject, DataFile sdrfDataFile) {
        File readmeFile = new File(PrideFilePathUtility.getReadMeFilePath(mongoPrideProject, prideRepoRootPath));
        try {
            log.info("Appending Readme file" + readmeFile.getPath());
            FileUtils.write(readmeFile, sdrfDataFile.getFileId() + "\t" + sdrfDataFile.getFileName() + "\t" +
                    sdrfDataFile.getFilePath().replace(prideRepoRootPath, ftpProtocolUrl + File.separator) + "\t"
                    + sdrfDataFile.getFileType() + "\t-\n", Charset.defaultCharset(), true);
            Process cpReadme = Runtime.getRuntime().exec(BECOME_PRIDE_ADM_CP + readmeFile.getAbsolutePath()
                    +" "+ readmeFile.getAbsolutePath()
                    .replace(prideRepoRootPath, ldcStagingBaseFolder)
                    .replace(GENERATED + File.separator, ""));
            Process cpSdrf = Runtime.getRuntime().exec(BECOME_PRIDE_ADM_CP + sdrfDataFile.getFile().getAbsolutePath()
                    +" "+ sdrfDataFile.getFile().getAbsolutePath()
                    .replace(prideRepoRootPath, ldcStagingBaseFolder)
                    .replace(SUBMITTED + File.separator, ""));
            try {
                log.info("Copying Readme file to staging");
                cpReadme.waitFor();
                log.info("Copying Sdrf file to staging");
                cpSdrf.waitFor();
            } catch (InterruptedException e) {
                log.error("Error copying to staging area" + "\n" + e.getMessage());
            }
        } catch (IOException e) {
            String errorMessage = "Error writing to" + readmeFile.getAbsolutePath();
            log.error(errorMessage +"\n" + e.getMessage());
            throw new RuntimeException(errorMessage);
        }
    }

    private void addToSubmissonFile(Submission submission, File submissionFile, DataFile sdrfDataFile) {
        try {
            if (!submission.containsDataFile(sdrfDataFile)) {
                Integer maxFileId = submission.getDataFiles()
                        .stream().map(dataFile -> dataFile.getFileId()).max(Integer::compareTo).get();
                sdrfDataFile.setFileId(maxFileId + 1);
                submission.addDataFile(sdrfDataFile);
                SubmissionFileWriter.write(submission, submissionFile);
            }
        } catch (SubmissionFileException e) {
            String errorMessage = "Error writing to" + submissionFile.getPath();
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }


    private void saveRowToFile(TsvWriter tsvWriter, Record sdrfRecord,
                               String sampleName, String sampleChecksum,
                               String sampleAccession, String[] headers) {
        for (String header : headers) {
            if (header.toLowerCase().equals(SOURCE_NAME)) {
                tsvWriter.addValue(SOURCE_NAME, sampleName);
            } else {
                tsvWriter.addValue(header, sdrfRecord.getString(header));
            }
        }
        tsvWriter.addValue(SAMPLE_CHECKSUM, sampleChecksum);
        tsvWriter.addValue(SAMPLE_ACCESSION, sampleAccession);
        tsvWriter.writeValuesToRow();
    }

    private TsvWriter getTsvWriter(String accession, File file, String[] headers) {
        List<String> headersList = new ArrayList<>();
        headersList.addAll(Arrays.asList(headers));
        headersList.add(SAMPLE_CHECKSUM);
        headersList.add(SAMPLE_ACCESSION);
        log.info("Creating output File " + file.getAbsolutePath());
        TsvWriter tsvWriter = new TsvWriter(file, new TsvWriterSettings());
        tsvWriter.writeHeaders(headersList);
        return tsvWriter;
    }

    private String getSdrfFilePath(String accession) {
        Optional<MongoPrideProject> mongoPrideProject = prideProjectMongoService.findByAccession(accession);
        return PrideFilePathUtility.getSubmittedFilesPath(mongoPrideProject.get(), prideRepoRootPath);
    }

    private Map<String, String> getSampleChecksumAccession(String accession) {
        List<MongoPrideSdrf> mongoPrideSdrfList = prideSdrfMongoService.findByProjectAccession(accession);
        Map<String, String> sampleChecksumAccession = new HashMap<>();
        if (mongoPrideSdrfList != null) {
            mongoPrideSdrfList.stream().forEach(
                    mongoPrideSdrf -> {
                        sampleChecksumAccession.put(mongoPrideSdrf.getSample().get(SAMPLE_CHECKSUM),
                                mongoPrideSdrf.getSample().get(SAMPLE_ACCESSION));
                    }
            );
        }
        return sampleChecksumAccession;
    }

    private Set<ExternalReference> getExternalReferences(String[] headers, Record sdrfObject, String projectAccession) {
        Set<ExternalReference> externalReferences = new HashSet<>();
        externalReferences.add(ExternalReference.build(PRIDE_ARCHIVE_PROJECT_URL + projectAccession));
        return null;
    }

    private Set<Relationship> getRelationShip(String[] headers, Record sdrfObject) {
        return null;
    }

    private Set<Attribute> getAttributes(String[] headers, Record sdrfObject) {
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(Attribute.build("project", "PRIDE"));
        for (String columnName : headers) {
            if (columnName.toLowerCase().startsWith("characteristics")) {
                attributes.add(Attribute.build(columnName.substring(16, columnName.length() - 1), sdrfObject.getString(columnName)));
            }
        }
        return attributes;
    }


    private Map<String, String> createSample(String headers[], Record sdrfObject, String sampleAccession, String sampleChecksum) {
        Set<String> columnNames = new HashSet<>();
        int count = 1;
        Map<String, String> sample = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            String columnName = headers[i];
            if (!columnNames.add(columnName)) {
                sample.put(columnName + "_" + count++, sdrfObject.getString(i));
            } else {
                sample.put(columnName, sdrfObject.getString(columnName));
            }
        }
        sample.put(SAMPLE_ACCESSION, sampleAccession);
        sample.put(SAMPLE_CHECKSUM, sampleChecksum);
        return sample;
    }

    private void saveSamplesToMongo(String accession, String fileChecksum,
                                    Map<String, String> sample) {
        samplesToSave.add(MongoPrideSdrf.builder()
                .projectAccession(accession)
                .filechecksum(fileChecksum)
                .sample(sample)
                .build());
        if (samplesToSave.size() >= 1000) {
            prideSdrfMongoService.saveSdrfList(samplesToSave);
            samplesToSave.clear();
        }
    }

    private Tasklet syncToFtpTasklet() {
        return (stepContribution, chunkContext) -> {
            log.info("Executing sync shell script: " + SYNC_SCRIPT);
            Process p = new ProcessBuilder(SYNC_SCRIPT).start();
            p.waitFor();
        /* correct 'snap-release pride' output behaviour:
            [INFO] Running snap-release for pride
            [SUCCESS] Snap-release finished for project pride
           anything else, or no output, is an error
        */
            InputStream inputStream = p.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            boolean syncSuccess = false;
            while ((line = bufferedReader.readLine()) != null) {
                log.info(line);
                if (line.contains("SUCCESS")) {
                    syncSuccess = true;
                }
            }
            bufferedReader.close();
            inputStream.close();
            if (!syncSuccess) {
                inputStream = p.getErrorStream();
                String msg = "Failed to sync project with LDC";
                log.error(msg);
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                while ((line = bufferedReader.readLine()) != null) {
                    log.error(line);
                }
                throw new IllegalStateException(msg);
            }
            return RepeatStatus.FINISHED;
        };
    }
}
