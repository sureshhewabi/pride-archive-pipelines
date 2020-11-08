package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import com.google.gson.JsonObject;
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
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.HashUtils;
import uk.ac.ebi.pride.mongodb.archive.model.sdrf.MongoPrideSdrf;
import uk.ac.ebi.pride.mongodb.archive.service.sdrf.PrideSdrfMongoService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Configuration
@Slf4j
@EnableBatchProcessing
public class SaveSdrfToBioSamplesAndMongoFromGitHubJob extends AbstractArchiveJob {

    public static final String SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO_GIT = "saveSdrfToBioSamplesAndMongoGitHub";
    public static final String READ_TSV_GIT = "readTsvGitHub";
    public static final String SAVE_TO_BIO_SAMPLES_GIT = "saveToBioSamplesGitHub";
    public static final String SAVE_TO_MONGO_GIT = "saveToMongoGitHub";
    public static final String PRIDE_DOMAIN = "self.pride";
    public static final String SOURCE_NAME = "source name";
    public static final String PRIDE_ARCHIVE_PROJECT_URL = "https://www.ebi.ac.uk/pride/archive/projects/";
    public static final String SAMPLE_CHECKSUM = "sampleChecksum";
    public static final String SAMPLE_ACCESSION = "sampleAccession";

    @Value("${githubFolder:#{null}}")
    private String folderPath;

    private Map<String, Map<String, List<Record>>> accessionToSdrfContents = new HashMap<>();

    private Map<String, Map<String, List<JSONObject>>> accessionToSdrfToSave = new HashMap<>();

    @Autowired
    private BioSamplesClient bioSamplesClient;

    @Autowired
    private PrideSdrfMongoService prideSdrfMongoService;

    @Bean
    public Job saveSdrfToBioSamplesAndMongoGitHub() {
        return jobBuilderFactory
                .get(SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO_GIT)
                .start(readTsv())
                .next(saveToBioSamples())
                .next(saveToMongo())
                .build();

    }

    private Step readTsv() {
        return stepBuilderFactory
                .get(READ_TSV_GIT)
                .tasklet(readTsvTasklet()).build();
    }

    private Step saveToBioSamples() {
        return stepBuilderFactory
                .get(SAVE_TO_BIO_SAMPLES_GIT)
                .tasklet(saveToBioSamplesTasklet()).build();

    }

    private Step saveToMongo() {
        return stepBuilderFactory
                .get(SAVE_TO_MONGO_GIT)
                .tasklet(saveToMongoTasklet()).build();
    }

    private Tasklet readTsvTasklet() {
        return (stepContribution, chunkContext) -> {
            try {
                Files.walk(Paths.get(folderPath), Integer.MAX_VALUE).skip(1).forEach(file -> {
                    if (file.getFileName().toString().endsWith(".tsv")) {
                        TsvParser tsvParser = new TsvParser(new TsvParserSettings());
                        String accession = file.getParent().getFileName().toString();
                        Map<String, List<Record>> fileToSdrfContents = accessionToSdrfContents.get(accession);
                        try {
                            if (fileToSdrfContents == null) {
                                fileToSdrfContents = new HashMap<>();
                            }
                            fileToSdrfContents.put(HashUtils.getSha1Checksum(file.toFile()), tsvParser.parseAllRecords(file.toFile()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        accessionToSdrfContents.put(accession, fileToSdrfContents);

                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

            return RepeatStatus.FINISHED;
        };


    }

    private Tasklet saveToBioSamplesTasklet() {
        return (stepContribution, chunkContext) -> {
            for (Map.Entry<String, Map<String, List<Record>>> accessionSdrfContent : accessionToSdrfContents.entrySet()) {
                String projectAccession = accessionSdrfContent.getKey();
                accessionToSdrfToSave.put(projectAccession, saveToBioSample(projectAccession, accessionSdrfContent.getValue()));
            }
            return RepeatStatus.FINISHED;
        };
    }

    private Map<String, List<JSONObject>> saveToBioSample(String projectAccession, Map<String, List<Record>> sdrfContentsToProcess) {
        System.out.println(projectAccession);
        Map<String, String> sampleChecksumAccession = getSampleChecksumAccession(projectAccession);
        Map<String, List<JSONObject>> checksumToSamplesMongo = new HashMap<>();
        for (Map.Entry<String, List<Record>> sdrfContent : sdrfContentsToProcess.entrySet()) {
            List<Record> sdrfObjects = sdrfContent.getValue();
            sdrfObjects.remove(0);
            List<JSONObject> samples = new ArrayList<>();
            for (Record sdrfObject : sdrfObjects) {
                String[] headers = sdrfObject.getMetaData().headers();
                String sampleName = projectAccession + "-" + sdrfObject.getString(SOURCE_NAME);
                Sample sample = Sample.build(sampleName,
                        null, PRIDE_DOMAIN, Instant.now(), null,
                        getAttributes(headers, sdrfObject), getRelationShip(headers, sdrfObject), getExternalReferences(projectAccession), SubmittedViaType.JSON_API);


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

                JsonObject sdrfObjectJson = createJsonObject(headers, sdrfObject, sampleAccession, sampleChecksum);
                sdrfObjectJson.addProperty(headers[0], sampleName);

                samples.add(new JSONObject(sdrfObjectJson.toString()));
            }
            checksumToSamplesMongo.put(sdrfContent.getKey(), samples);
        }
        return checksumToSamplesMongo;
    }


    private Map<String, String> getSampleChecksumAccession(String accession) {
        MongoPrideSdrf mongoPrideSdrf = prideSdrfMongoService.findByProjectAccession(accession);
        Map<String, String> sampleChecksumAccession = new HashMap<>();
        if (mongoPrideSdrf != null) {
            mongoPrideSdrf.getSdrf().values().stream().flatMap(content -> content.stream()).forEach(
                    sdrfContent -> {
                        sampleChecksumAccession.put(sdrfContent.get(SAMPLE_CHECKSUM).toString(), sdrfContent.get(SAMPLE_ACCESSION).toString());
                    }
            );
        }
        return sampleChecksumAccession;
    }

    private Set<ExternalReference> getExternalReferences(String projectAccession) {
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
            if (columnName.contains("characteristics")) {
                attributes.add(Attribute.build(columnName.substring(16, columnName.length() - 1), sdrfObject.getString(columnName)));
            }
        }
        return attributes;
    }


    private JsonObject createJsonObject(String headers[], Record sdrfObject, String sampleAccession, String sampleChecksum) {
        Set<String> columnNames = new HashSet<>();
        int count = 1;
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < headers.length; i++) {
            String columnName = headers[i];
            if (!columnNames.add(columnName)) {
                jsonObject.addProperty(columnName + "_" + count++, sdrfObject.getString(i));
            } else {
                jsonObject.addProperty(columnName, sdrfObject.getString(columnName));
            }
        }
        jsonObject.addProperty(SAMPLE_ACCESSION, sampleAccession);
        jsonObject.addProperty(SAMPLE_CHECKSUM, sampleChecksum);
        return jsonObject;
    }

    private Tasklet saveToMongoTasklet() {
        return (stepContribution, chunkContext) -> {
            if (accessionToSdrfToSave.size() == 0) {
                return RepeatStatus.FINISHED;
            }
            accessionToSdrfToSave.entrySet().stream().forEach(entry -> {
                String projectAccession = entry.getKey();
                MongoPrideSdrf mongoPrideSdrf = prideSdrfMongoService.findByProjectAccession(projectAccession);
                ObjectId id = null;
                if (mongoPrideSdrf != null) {
                    id = mongoPrideSdrf.getId();
                }
                prideSdrfMongoService.saveSdrf(MongoPrideSdrf.builder()
                        .id(id)
                        .projectAccession(projectAccession)
                        .sdrf(entry.getValue())
                        .build());
            });

            return RepeatStatus.FINISHED;
        };
    }

}
