package uk.ac.ebi.pride.archive.pipeline.tasklets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import uk.ac.ebi.pride.archive.repo.repos.project.ProjectRepository;
import uk.ac.ebi.pride.data.io.SubmissionFileParser;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.proteinidentificationindex.mongo.search.model.MongoProteinIdentification;
import uk.ac.ebi.pride.proteinidentificationindex.mongo.search.service.MongoProteinIdentificationSearchService;
import uk.ac.ebi.pride.proteinidentificationindex.search.model.ProteinIdentification;
import uk.ac.ebi.pride.proteinidentificationindex.search.service.ProteinIdentificationSearchService;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static uk.ac.ebi.pride.archive.pipeline.tasklets.LaunchIndividualEbeyeXmlTasklet.launchIndividualEbeyeXmlGenerationForProjectAcc;

@Component

public class GenerateEbeyeXmlTasklet extends AbstractTasklet {

    public static final Logger logger = LoggerFactory.getLogger(GenerateEbeyeXmlTasklet.class);

    private File submissionFile;
    private File outputDirectory;
    private String projectAccession;
    @Autowired
    PrideProjectMongoService prideProjectMongoService;
    private ProteinIdentificationSearchService proteinIdentificationSearchService;
    private MongoProteinIdentificationSearchService mongoProteinIdentificationSearchService;
    private boolean processAll;
    private List<String> exceptionsLaunchingProjects;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        exceptionsLaunchingProjects = Collections.synchronizedList(new ArrayList<String>());
        if (processAll) {
            prideProjectMongoService.getAllProjectAccessions().parallelStream().forEach(projAcc -> {
                launchIndividualEbeyeXmlGenerationForProjectAcc(projAcc, prideProjectMongoService, exceptionsLaunchingProjects);
            });
            if (!CollectionUtils.isEmpty(exceptionsLaunchingProjects)) {
                exceptionsLaunchingProjects.parallelStream().forEach(s -> logger.error("Problems launching EBeye generation for: " + s));
                throw new JobExecutionException("Unable to launch individual EBeye generation jobs");
            }
        } else {
            generateEBeye(projectAccession, submissionFile);
        }
        logger.info("Finished generating EBeye XML.");
        return RepeatStatus.FINISHED;
    }

    /**
     * This method looks up a project from the provided accession number, and if it is not partial, i.e. with protein IDs,
     * then these are mapped to Uniprot and Ensembl. EBeye XML is then generated for this project and saved to an output file
     * in the PRIDE Archive's EBeye XML directory.
     *
     * @param projectAcc the project's accession number to generate EBeye XML for
     * @throws Exception any problem during the EBeye generation process
     */
    private void generateEBeye(String projectAcc, File submissionFile) throws Exception {
        long proteinCount = proteinIdentificationSearchService.countByProjectAccession(projectAcc);
        List<MongoProteinIdentification> proteinResults = new ArrayList<>();
        final int MAX_PAGE_SIZE = 1000; // 10 000 may cause timeouts
        if (proteinCount > MAX_PAGE_SIZE) {
            int i = 0;
            int currentPage = 0;
            List<ProteinIdentification> proteinList;
            while (i < proteinCount) {
                proteinList = proteinIdentificationSearchService.findByProjectAccession(projectAcc,
                        new PageRequest(currentPage, MAX_PAGE_SIZE)).getContent();
                if (proteinList.size() > 0) {
                    proteinResults.addAll(mongoProteinIdentificationSearchService.findByIdIn(proteinList.
                            stream().
                            map(ProteinIdentification::getId).
                            collect(Collectors.toCollection(ArrayList::new))));
                }
                i += MAX_PAGE_SIZE;
                currentPage++;
            }
        } else {
            if (proteinCount > 0) {
                proteinResults = mongoProteinIdentificationSearchService.findByIdIn(
                        proteinIdentificationSearchService.findByProjectAccession(projectAcc).
                                stream().
                                map(ProteinIdentification::getId).
                                collect(Collectors.toCollection(ArrayList::new)));
            }
        }
        HashMap<String, String> proteins = new HashMap<>();
        if (proteinResults.size() > 0) {
            for (MongoProteinIdentification proteinId : proteinResults) {
                if (proteinId.getUniprotMapping() != null && !proteinId.getUniprotMapping().isEmpty()) {
                    if (!proteins.containsKey(proteinId.getUniprotMapping())) {
                        proteins.put(proteinId.getUniprotMapping(), "uniprot");
                    }
                }
                if (proteinId.getEnsemblMapping() != null && !proteinId.getEnsemblMapping().isEmpty()) {
                    if (!proteins.containsKey(proteinId.getEnsemblMapping())) {
                        proteins.put(proteinId.getEnsemblMapping(), "ensembl");
                    }
                }
            }
        }

        GenerateEBeyeXMLNew generateEBeyeXMLNew = new GenerateEBeyeXMLNew(projectRepository.findByAccession(projectAcc),
                SubmissionFileParser.parse(submissionFile),outputDirectory,proteins,true);
        generateEBeyeXMLNew.generate();






        /*GenerateEBeyeXML generateEBeyeXML = new GenerateEBeyeXML(projectRepository.findByAccession(projectAcc),
                SubmissionFileParser.parse(submissionFile), outputDirectory, proteins, true);
        logger.info("About to generate EB-eye XML file for: " + projectAcc);
        generateEBeyeXML.generate();*/
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(projectRepository, "Project repo cannot be null.");
        Assert.notNull(outputDirectory, "Output directory cannot be null.");
    }

    public void setSubmissionFile(File submissionFile) {
        this.submissionFile = submissionFile;
    }

    public void setProjectAccession(String projectAccession) {
        this.projectAccession = projectAccession;
    }

    public void setProjectRepository(ProjectRepository projectRepository) {
        this.projectRepository = projectRepository;
    }

    public void setOutputDirectory(File outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public void setProteinIdentificationSearchService(ProteinIdentificationSearchService proteinIdentificationSearchService) {
        this.proteinIdentificationSearchService = proteinIdentificationSearchService;
    }

    public void setMongoProteinIdentificationSearchService(MongoProteinIdentificationSearchService mongoProteinIdentificationSearchService) {
        this.mongoProteinIdentificationSearchService = mongoProteinIdentificationSearchService;
    }

    public void setProcessAll(Boolean processAll) {
        if (processAll != null) {
            this.processAll = processAll;
        }
    }
}

