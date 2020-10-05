package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParam;
import uk.ac.ebi.pride.archive.dataprovider.utils.MSFileTypeConstants;
import uk.ac.ebi.pride.archive.dataprovider.utils.ProjectFolderSourceConstants;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.RepoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.SolrApiClientConfig;
import uk.ac.ebi.pride.archive.pipeline.core.transformers.PrideProjectTransformer;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.HashUtils;
import uk.ac.ebi.pride.archive.repo.client.FileRepoClient;
import uk.ac.ebi.pride.archive.repo.client.ProjectRepoClient;
import uk.ac.ebi.pride.archive.repo.models.file.ProjectFile;
import uk.ac.ebi.pride.archive.repo.models.project.Project;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.solr.api.client.SolrProjectClient;
import uk.ac.ebi.pride.solr.commons.PrideSolrProject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({RepoConfig.class, ArchiveMongoConfig.class, DataSourceConfiguration.class, SolrApiClientConfig.class})
public class SanityCheckJob extends AbstractArchiveJob {

    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    ProjectRepoClient projectRepoClient;

    @Autowired
    FileRepoClient fileRepoClient;

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    private SolrProjectClient solrProjectClient;

    @Value("${ftp.protocol.url}")
    private String ftpUrl;

    @Value("${aspera.protocol.url}")
    private String asperaUrl;

    private Map<String, Long> taskTimeMap = new HashMap<>();

    private String[] projectAccessions;
    private boolean fixFilesOptionSet = false;
    private boolean fixProjectsOptionSet = false;

    @Bean
    @StepScope
    public Tasklet initSanityCheckJob(@Value("#{jobParameters['projects']}") String projects,
                                      @Value("#{jobParameters['fixProjects']}") Boolean fixProjects,
                                      @Value("#{jobParameters['fixFiles']}") Boolean fixFiles) {
        return (stepContribution, chunkContext) ->
        {
            if (projects != null) {
                this.projectAccessions = projects.split(",");
            }

            if (fixProjects != null && fixProjects) {
                this.fixProjectsOptionSet = true;
            }
            if (fixFiles != null && fixFiles) {
                this.fixFilesOptionSet = true;
            }

            log.info(String.format("==================>>>>>>> initSanityCheckJobJob - Run the job for Project %s", Arrays.toString(projectAccessions)));
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Job sanityCheckJobBean() {
        return jobBuilderFactory
                .get("sanityCheckJobBean")
                .start(stepBuilderFactory
                        .get("initSanityCheckJobJob")
                        .tasklet(initSanityCheckJob(null, null, null))
                        .build())
                .next(sanityCheckStep())
                .next(sanityCheckPrintTraceStep())
                .build();
    }

    @Bean
    public Step sanityCheckPrintTraceStep() {
        return stepBuilderFactory
                .get("sanityCheckPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step sanityCheckStep() {
        return stepBuilderFactory
                .get("sanityCheckStep")
                .tasklet((stepContribution, chunkContext) -> {

                    long initInsertPeptides = System.currentTimeMillis();

                    if (projectAccessions == null || projectAccessions.length == 0) {
                        Set<String> mongoPrjAccessions = getMongoProjectAccessions();
                        List<String> allPublicAccessions = projectRepoClient.getAllPublicAccessions();
                        Set<String> oracleAccessions = new HashSet<>(allPublicAccessions);

                        Set<String> onlyInMongo = new HashSet<>(mongoPrjAccessions);
                        onlyInMongo.removeAll(oracleAccessions);
                        Set<String> onlyInOracle = new HashSet<>(oracleAccessions);
                        onlyInOracle.removeAll(mongoPrjAccessions);

                        if(!onlyInMongo.isEmpty()) {
                            log.error("====[Only in Mongo]: " + Arrays.toString(onlyInMongo.toArray()));
                        }
                        if(!onlyInOracle.isEmpty()) {
                            log.error("====[Only in Oracle]: " + Arrays.toString(onlyInOracle.toArray()));
                        }

                        mongoPrjAccessions.forEach(this::compareProjectsAndFix);

                    } else {
                        Arrays.stream(projectAccessions).forEach(this::compareProjectsAndFix);
                    }

                    taskTimeMap.put("SanityCheckJob", System.currentTimeMillis() - initInsertPeptides);

                    return RepeatStatus.FINISHED;
                }).build();
    }

    private Set<String> getMongoProjectAccessions() {
        Set<String> mongoProjectAccessions = prideProjectMongoService.getAllProjectAccessions();
        log.info("Number of MongoDB projects: " + mongoProjectAccessions.size());
        return mongoProjectAccessions;
    }

    private void updateSolrProject(String prjAccession, Set<String> fileNames) throws IOException {
        Optional<PrideSolrProject> prideSolrProjectOptional = solrProjectClient.findByAccession(prjAccession);
        if (!prideSolrProjectOptional.isPresent()) {
            return;
        }
        PrideSolrProject prideSolrProject = prideSolrProjectOptional.get();

        prideSolrProject.setProjectFileNames(fileNames);
        solrProjectClient.update(prideSolrProject);
        log.info("updated solr project: " + prjAccession);
    }

    private void compareProjectsAndFix(String accession) {
        log.info("PROCESSING : " + accession);
        String errorLogPrefix = "====[" + accession + "] ";
        try {
            Project oracleProject = projectRepoClient.findByAccession(accession);

            Optional<MongoPrideProject> mongoProjectOptional = prideProjectMongoService.findByAccession(accession);
            if (oracleProject != null && oracleProject.isPublicProject() && mongoProjectOptional.isPresent()) {
                MongoPrideProject transformedOracleProject = PrideProjectTransformer.transformOracleToMongo(oracleProject);
                MongoPrideProject mongoProject = mongoProjectOptional.get();

                boolean mongoUpdated = false;

            /*Set<String> countries = new HashSet<>(mongoProject.getCountries());
            countries.addAll(transformedOracleProject.getCountries());
            transformedOracleProject.setCountries(new ArrayList<>(countries));
            if (!transformedOracleProject.getTitle().equals(mongoProject.getTitle())) {
                mongoProject.setTitle(transformedOracleProject.getTitle());
                mongoUpdated = true;
            }*/

                if (!Objects.equals(transformedOracleProject.getSubmissionDate(), mongoProject.getSubmissionDate())) {
                    mongoProject.setSubmissionDate(transformedOracleProject.getSubmissionDate());
                    mongoUpdated = true;
                }
                if (!Objects.equals(transformedOracleProject.getPublicationDate(), mongoProject.getPublicationDate())) {
                    mongoProject.setPublicationDate(transformedOracleProject.getPublicationDate());
                    mongoUpdated = true;
                }
                if (!Objects.equals(transformedOracleProject.getUpdatedDate(), mongoProject.getUpdatedDate())) {
                    mongoProject.setUpdatedDate(transformedOracleProject.getUpdatedDate());
                    mongoUpdated = true;
                }

                if (mongoUpdated) {
                    log.error(" ==== Mongo project Dates Mismatched : " + accession + " ==== ");
                    if (fixProjectsOptionSet) {
                        mongoProject = prideProjectMongoService.update(mongoProject).get();
                        log.info("==== fixed mongo project : " + accession + " ====");
                    }
                }

            /*if (!transformedOracleProject.equals(mongoProject)) {
                log.error(" ==== Mongo project Mismatched : " + accession + "==== ");
                log.info("transformedOracleProject : " + transformedOracleProject);
                log.info("mongoPrideProject : " + mongoProject);
            }*/

                Set<MongoPrideFile> mongoFiles = new HashSet<>(prideFileMongoService.findFilesByProjectAccession(accession));
                Map<String, String> checkSumMap = new HashMap<>();
                mongoFiles.forEach(m -> checkSumMap.put(m.getAccession(), m.getChecksum()));

                List<ProjectFile> oracleFiles = fileRepoClient.findAllByProjectId(oracleProject.getId());
                Set<MongoPrideFile> transfromedOracleFiles = oracleFiles.stream().map(o -> transformOracleFileToMongo(o, oracleProject, checkSumMap)).collect(Collectors.toSet());

                Map<String, MongoPrideFile> mongoFilesMap = new HashMap<>();
                mongoFiles.forEach(m -> mongoFilesMap.put(m.getAccession(), m));

                Map<String, MongoPrideFile> transfromedOracleFilesMap = new HashMap<>();
                transfromedOracleFiles.forEach(m -> transfromedOracleFilesMap.put(m.getAccession(), m));

                if (!mongoFiles.equals(transfromedOracleFiles)) {
                    log.info(" ==== Mongo files mismatched for project : " + accession + " ==== ");
                }

                boolean fixedFiles = false;
                for (MongoPrideFile m : mongoFiles) {
                    boolean fileUpdate = false;
                    MongoPrideFile transformedFile = transfromedOracleFilesMap.get(m.getAccession());
                    if (!Objects.equals(m.getSubmissionDate(), transformedFile.getSubmissionDate())) {
                        m.setSubmissionDate(transformedFile.getSubmissionDate());
                        fileUpdate = true;
                    }
                    if (!Objects.equals(m.getPublicationDate(), transformedFile.getPublicationDate())) {
                        m.setPublicationDate(transformedFile.getPublicationDate());
                        fileUpdate = true;
                    }
                    if (!Objects.equals(m.getUpdatedDate(), transformedFile.getUpdatedDate())) {
                        m.setUpdatedDate(transformedFile.getUpdatedDate());
                        fileUpdate = true;
                    }
                    if (!Objects.equals(m.getPublicFileLocations(), transformedFile.getPublicFileLocations())) {
                        m.setPublicFileLocations(transformedFile.getPublicFileLocations());
                        fileUpdate = true;
                    }
                    if (!Objects.equals(m.getFileSizeBytes(), transformedFile.getFileSizeBytes())) {
                        m.setFileSizeBytes(transformedFile.getFileSizeBytes());
                        fileUpdate = true;
                    }
                    if (fileUpdate && fixFilesOptionSet) {
                        fixedFiles = true;
                        prideFileMongoService.save(m);
                    }
                }

                if (fixedFiles) {
                    log.info("==== fixed mongo files for : " + accession + " ====");
                }

                HashSet<MongoPrideFile> fixedMongoFiles = new HashSet<>(prideFileMongoService.findFilesByProjectAccession(accession));
                if (!fixedMongoFiles.equals(transfromedOracleFiles)) {
                    if (fixedFiles) {
                        log.error(errorLogPrefix + "Even after fixing, Mongo files mismatched");
                    }

                    //debug log to identify the differences
                    List<MongoPrideFile> collect = mongoFiles.stream()
                            .filter(m -> !mongoFilesMap.get(m.getAccession()).equals(transfromedOracleFilesMap.get(m.getAccession())))
                            .collect(Collectors.toList());
                    if (collect.size() > 0) {
                        log.error(errorLogPrefix + "mismatched mongo files : " + collect);
                        List<MongoPrideFile> collect1 = collect.stream().map(c -> transfromedOracleFilesMap.get(c.getAccession())).collect(Collectors.toList());
                        log.error(errorLogPrefix + "reference transformedOracleFiles : " + collect1);
                    }

                    //case where one or more files are missing in mongo..
                    //checksum in transfromedOracleFiles is taken from from mongo files. so, if the mongo file is missing then it's null for transfromedOracleFile
                    final List<MongoPrideFile> missingMongoFiles = transfromedOracleFiles.stream().filter(o -> o.getChecksum() == null).collect(Collectors.toList());
                    if (missingMongoFiles.size() == transfromedOracleFiles.size()) {
                        log.error(errorLogPrefix + "All mongo files are missing");
                    } else {
                        List<String> missingFiles = missingMongoFiles.stream().map(m -> m.getFileSourceFolder() + "/" + m.getFileName()).collect(Collectors.toList());
                        if (missingFiles.size() > 0) {
                            log.error(errorLogPrefix + "Missing mongo files: " + missingFiles);
                        } else {
                            //this is a case where oracle has duplicate file entries
                            log.error(errorLogPrefix + "Oracle has duplicate file entries");
                        }
                    }
                }

          /*  PrideSolrProject solrProject = solrProjectService.findByAccession(accession);

            PrideSolrProject transformedSolrProject = PrideProjectTransformer.transformProjectMongoToSolr(mongoProject);

            if (transformedSolrProject.getAdditionalAttributesStrings() != null && transformedSolrProject.getAdditionalAttributesStrings().isEmpty()) {
                solrProject.setAdditionalAttributesFromCvParams(Collections.emptyList());
            }

            if (transformedSolrProject.getProjectTags() != null && transformedSolrProject.getProjectTags().isEmpty()) {
                solrProject.setProjectTags(Collections.emptyList());
            }

            if (transformedSolrProject.getLabPIs() != null && transformedSolrProject.getLabPIs().isEmpty()) {
                solrProject.setLabPIs(Collections.emptySet());
            }

            if (transformedSolrProject.getKeywords() != null && transformedSolrProject.getKeywords().isEmpty()) {
                solrProject.setKeywords(Collections.emptyList());
            }

            if (transformedSolrProject.getOtherOmicsLink() != null && transformedSolrProject.getOtherOmicsLink().isEmpty()) {
                solrProject.setOtherOmicsLinks(Collections.emptySet());
            }

            if (transformedSolrProject.getSubmitters() != null && transformedSolrProject.getSubmitters().isEmpty()) {
                solrProject.setSubmittersFromContacts(null);
            }

            if (transformedSolrProject.getAffiliations() != null && transformedSolrProject.getAffiliations().isEmpty()) {
                solrProject.setAffiliations(Collections.emptySet());
            }

            if (transformedSolrProject.getInstruments() != null && transformedSolrProject.getInstruments().isEmpty()) {
                solrProject.setInstrumentsFromCvParam(Collections.emptyList());
            }

            if (transformedSolrProject.getSoftwares() != null && transformedSolrProject.getSoftwares().isEmpty()) {
                solrProject.setSoftwaresFromCvParam(Collections.emptyList());
            }

            if (transformedSolrProject.getQuantificationMethods() != null && transformedSolrProject.getQuantificationMethods().isEmpty()) {
                solrProject.setQuantificationMethodsFromCvParams(Collections.emptyList());
            }

            if (transformedSolrProject.getAllCountries() != null && transformedSolrProject.getAllCountries().isEmpty()) {
                solrProject.setAllCountries(Collections.emptySet());
            }

            if (transformedSolrProject.getExperimentalFactors() != null && transformedSolrProject.getExperimentalFactors().isEmpty()) {
                solrProject.setExperimentalFactors(Collections.emptyList());
            }

            if (transformedSolrProject.getSampleAttributes() != null && transformedSolrProject.getSampleAttributes().isEmpty()) {
                solrProject.setSampleAttributes(Collections.emptyList());
            }

            if (transformedSolrProject.getOrganisms() != null && transformedSolrProject.getOrganisms().isEmpty()) {
                solrProject.setOrganisms(Collections.emptySet());
            }

            if (transformedSolrProject.getOrganisms_facet() != null && transformedSolrProject.getOrganisms_facet().isEmpty()) {
                solrProject.setOrganisms_facet(Collections.emptySet());
            }

            if (transformedSolrProject.getOrganismPart() != null && transformedSolrProject.getOrganismPart().isEmpty()) {
                solrProject.setOrganismPart(Collections.emptySet());
            }

            if (transformedSolrProject.getOrganismPart_facet() != null && transformedSolrProject.getOrganismPart_facet().isEmpty()) {
                solrProject.setOrganismPart_facet(Collections.emptySet());
            }

            if (transformedSolrProject.getDiseases() != null && transformedSolrProject.getDiseases().isEmpty()) {
                solrProject.setDiseases(Collections.emptySet());
            }

            if (transformedSolrProject.getDiseases_facet() != null && transformedSolrProject.getDiseases_facet().isEmpty()) {
                solrProject.setDiseases_facet(Collections.emptySet());
            }

            if (transformedSolrProject.getReferences() != null && transformedSolrProject.getReferences().isEmpty()) {
                solrProject.setReferences(Collections.emptySet());
            }

            if (transformedSolrProject.getProjectFileNames() != null && transformedSolrProject.getProjectFileNames().isEmpty()) {
                solrProject.setProjectFileNames(Collections.emptySet());
            }

            Set<String> fileNames = mongoFiles.stream().map(MongoPrideFile::getFileName).collect(Collectors.toSet());
            transformedSolrProject.setProjectFileNames(fileNames);
            if (!transformedSolrProject.equals(solrProject)) {
                log.error(" ==== Solr project Mismatched : " + accession + " ==== ");
                log.info("transformedSolrProject : " + transformedSolrProject);
                log.info("solrProject : " + solrProject);
//                transformedSolrProject.setId((String)solrProject.getId());
//                solrProjectService.update(transformedSolrProject);
            }*/
            }
        } catch (Exception e) {
            log.error(errorLogPrefix + e.getMessage());
            log.error(e.getMessage(), e);
        }
    }

    private MongoPrideFile transformOracleFileToMongo(ProjectFile oracleFile, Project oracleProject, Map<String, String> checkSumMap) {
        MSFileTypeConstants fileType = MSFileTypeConstants.OTHER;
        for (MSFileTypeConstants currentFileType : MSFileTypeConstants.values())
            if (currentFileType.getFileType().getName().equalsIgnoreCase(oracleFile.getFileType().getName()))
                fileType = currentFileType;
        String folderName = Objects.requireNonNull(ProjectFolderSourceConstants.fromTypeString(oracleFile.getFileSource().name())).getFolderName();
        Set<CvParam> publicURLs = oracleProject.isPublicProject() ? PrideProjectTransformer.createPublicFileLocations(oracleFile.getFileName(),
                folderName, oracleProject.getPublicationDate(), oracleProject.getAccession(), ftpUrl, asperaUrl) : Collections.emptySet();

        String accession = HashUtils.getSha256Checksum(oracleProject.getAccession() + folderName + oracleFile.getFileName());

        return MongoPrideFile.builder()
                .accession(accession)
                .fileName(oracleFile.getFileName())
                .fileCategory(new CvParam(fileType.getFileType().getCv().getCvLabel(), fileType.getFileType().getCv().getAccession(),
                        fileType.getFileType().getCv().getName(), fileType.getFileType().getCv().getValue()))
                .fileSourceFolder(oracleFile.getFileSource().name())
                .projectAccessions(Collections.singleton(oracleProject.getAccession()))
                .fileSizeBytes(oracleFile.getFileSize())
                .publicationDate(oracleProject.getPublicationDate())
                .fileSourceType(oracleFile.getFileSource().name())
                .fileSourceFolder(folderName)
                .publicFileLocations(publicURLs)
                .submissionDate(oracleProject.getSubmissionDate())
                .updatedDate(oracleProject.getUpdateDate())
                .checksum(checkSumMap.get(accession)) //Checksum is not calculated again.
                .build();
    }
}
