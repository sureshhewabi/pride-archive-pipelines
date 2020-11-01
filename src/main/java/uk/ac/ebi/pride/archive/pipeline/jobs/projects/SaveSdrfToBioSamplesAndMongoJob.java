package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.json.JSONObject;
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
import uk.ac.ebi.pride.archive.dataprovider.file.ProjectFileType;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.HashUtils;
import uk.ac.ebi.pride.archive.pipeline.utility.PrideFilePathUtility;
import uk.ac.ebi.pride.data.io.SubmissionFileParser;
import uk.ac.ebi.pride.data.model.DataFile;
import uk.ac.ebi.pride.data.model.Submission;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.model.sdrf.MongoPrideSdrf;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.sdrf.PrideSdrfMongoService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Configuration
@Slf4j
@EnableBatchProcessing
public class SaveSdrfToBioSamplesAndMongoJob extends AbstractArchiveJob {

    public static final String SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO = "saveSdrfToBioSamplesAndMongo";
    public static final String READ_TSV = "readTsv";
    public static final String SAVE_TO_BIO_SAMPLES = "saveToBioSamples";
    public static final String SAVE_TO_MONGO = "saveToMongo";
    public static final String PRIDE_DOMAIN = "self.pride";
    public static final String SOURCE_NAME = "source name";

    @Value("${accession:#{null}}")
    private String projectAccession;

    @Value("${pride.archive.data.path}")
    private String prideRepoRootPath;

    @Autowired
    private BioSamplesClient bioSamplesClient;

    @Autowired
    private PrideSdrfMongoService prideSdrfMongoService;

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    private Map<String, List<Record>> sdrfContents = new HashMap<>();

    private Map<String, String> sdrfFileChecksum = new HashMap<>();

    List<JSONObject> samples = new ArrayList();

    private MongoPrideSdrf mongoPrideSdrf;

    @Bean
    public Job sdrfSaveToBioSamplesAndMongo() {
        return jobBuilderFactory
                .get(SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO)
                .start(readTsv())
                .next(saveToBioSamples())
                .next(saveToMongo())
                .build();

    }

    private Step readTsv() {
        return stepBuilderFactory
                .get(READ_TSV)
                .tasklet(readTsvTasklet()).build();
    }

    private Step saveToBioSamples() {
        return stepBuilderFactory
                .get(SAVE_TO_BIO_SAMPLES)
                .tasklet(saveToBioSamplesTasklet()).build();

    }

    private Step saveToMongo() {
        return stepBuilderFactory
                .get(SAVE_TO_MONGO)
                .tasklet(saveToMongoTasklet()).build();
    }

    private Tasklet readTsvTasklet() {
        return (stepContribution, chunkContext) -> {
            Optional<MongoPrideProject> mongoPrideProject = prideProjectMongoService.findByAccession(projectAccession);
            String submissionFilePath = PrideFilePathUtility.getSubmissionFilePath(mongoPrideProject.get(), prideRepoRootPath);
            Submission submission = SubmissionFileParser.parse(new File(submissionFilePath));
            mongoPrideSdrf = prideSdrfMongoService.findByProjectAccession(projectAccession);


            for (DataFile dataFile : submission.getDataFiles()) {
                if (dataFile.getFileType().equals(ProjectFileType.EXPERIMENTAL_DESIGN)) {
                    String sdrfFilePath = PrideFilePathUtility.getSubmittedFilesPath(mongoPrideProject.get(), prideRepoRootPath) +
                             dataFile.getFileName();
                    String fileChecksum = HashUtils.getSha1Checksum(new File(sdrfFilePath));


                    if (checkFilesAlreadySaved(fileChecksum)) {
                        return RepeatStatus.FINISHED;
                    }
                    sdrfFileChecksum.put(fileChecksum, sdrfFilePath);

                    try (BufferedReader stream = new BufferedReader(new FileReader(sdrfFilePath))) {
                        TsvParser tsvParser = new TsvParser(new TsvParserSettings());
                        sdrfContents.put(fileChecksum, tsvParser.parseAllRecords(stream));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }


                }
            }
            return RepeatStatus.FINISHED;

        };
    }


    /*
     This method is to check whether the project is being republished with same sdrf.
     */
    private boolean checkFilesAlreadySaved(String fileChecksum) {
        if (mongoPrideSdrf != null) {
            Map<String, String> fileChecksums = mongoPrideSdrf.getSdrfFileCheckSum();
            if (fileChecksums.containsKey(fileChecksum)) {
                return true;
            }
        }
        return false;
    }

    private Tasklet saveToBioSamplesTasklet() {
        return (stepContribution, chunkContext) -> {
            Map<String, String> sampleChecksumAccession = getSampleChecksumAccession();
            for (Map.Entry<String, List<Record>> sdrfContent : sdrfContents.entrySet()) {
                List<Record> sdrfObjects = sdrfContent.getValue();
                sdrfObjects.remove(0);
                for (Record sdrfObject : sdrfObjects) {
                    String[] headers = sdrfObject.getMetaData().headers();
                    String sampleName = projectAccession + "_" + sdrfObject.getString(SOURCE_NAME);
                    Sample sample = Sample.build(sampleName,
                            null, PRIDE_DOMAIN, Instant.now(), null,
                            getAttributes(headers, sdrfObject), getRelationShip(headers, sdrfObject), getExternalReferences(headers, sdrfObject), SubmittedViaType.JSON_API);


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

                    String sdrfObjectString = reflectToString(headers, sdrfObject, sampleAccession, sampleChecksum);
                    sdrfObjectString = sdrfObjectString.replace(sdrfObject.getString(SOURCE_NAME), sampleName);

                    samples.add(new JSONObject(sdrfObjectString));
                }

            }


            return RepeatStatus.FINISHED;
        };
    }

    private Map<String, String> getSampleChecksumAccession() {
        Map<String, String> sampleChecksumAccession = new HashMap<>();
        if (mongoPrideSdrf != null) {
            mongoPrideSdrf.getSdrf().stream().forEach(
                    sdrfContent -> {
                        sampleChecksumAccession.put(sdrfContent.get("sampleChecksum").toString(), sdrfContent.get("sampleAccession").toString());
                    }
            );
        }
        return sampleChecksumAccession;
    }

    private Set<ExternalReference> getExternalReferences(String[] headers, Record sdrfObject) {
        Set<ExternalReference> externalReferences = new HashSet<>();
        externalReferences.add(ExternalReference.build("projectUrl"));
        return null;
    }

    private Set<Relationship> getRelationShip(String[] headers, Record sdrfObject) {
        return null;
    }

    private Set<Attribute> getAttributes(String[] headers, Record sdrfObject) {
        Set<Attribute> attributes = new HashSet<>();
        for (String columnName : headers) {
            if (columnName.contains("characteristics")) {
                attributes.add(Attribute.build(columnName.substring(16, columnName.length() - 1), sdrfObject.getString(columnName)));
            }
        }
        return attributes;
    }


    private String reflectToString(String headers[], Record sdrfObject, String sampleAccession, String sampleChecksum) throws IllegalAccessException {
        StringBuilder result = new StringBuilder();
        result.append(" {");
        Set<String> columnNames = new HashSet<>();
        int count = 1;
        for (int i = 0; i < headers.length; i++) {
            String columnName = headers[i];
            if (!columnNames.add(columnName)) {
                result.append("\n").append("\"" + columnName + "_" + count++ + "\"").append(":").append("\"" + sdrfObject.getString(i) + "\"").append(", ");
            } else {
                result.append("\n").append("\"" + columnName + "\"").append(":").append("\"" + sdrfObject.getString(columnName) + "\"").append(", ");
            }
        }
        result.append("\n").append("\"sampleAccession\"").append(":").append("\"" + sampleAccession + "\"").append(", ");
        result.append("\n").append("\"sampleChecksum\"").append(":").append("\"" + sampleChecksum + "\"");
        return result.append("\n}").toString();
    }

    private Tasklet saveToMongoTasklet() {
        return (stepContribution, chunkContext) -> {
            if (samples.size() == 0) {
                return RepeatStatus.FINISHED;
            }
            ObjectId id = null;
            if (mongoPrideSdrf != null) {
                id = mongoPrideSdrf.getId();
            }
            MongoPrideSdrf mongoPrideSdrf = MongoPrideSdrf.builder()
                    .id(id)
                    .projectAccession(projectAccession)
                    .sdrf(samples)
                    .sdrfFileCheckSum(sdrfFileChecksum)
                    .build();
            prideSdrfMongoService.saveSdrf(mongoPrideSdrf);
            return RepeatStatus.FINISHED;
        };
    }

}
