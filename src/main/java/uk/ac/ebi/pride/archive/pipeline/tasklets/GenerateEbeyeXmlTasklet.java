package uk.ac.ebi.pride.archive.pipeline.tasklets;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import uk.ac.ebi.pride.archive.pipeline.utility.BackupUtil;
import uk.ac.ebi.pride.archive.pipeline.utility.PrideFilePathUtility;
import uk.ac.ebi.pride.data.io.SubmissionFileParser;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;
import uk.ac.ebi.pride.tools.protein_details_fetcher.ProteinDetailFetcher;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@StepScope
@Component
@Slf4j
public class GenerateEbeyeXmlTasklet extends AbstractTasklet {

    @Value("${pride.data.backup.path}")
    String backupPath;

    @Value("${pride.archive.data.path}")
    private String prideRepoRootPath;

    @Value("${pride.ebeye.dir:''}")
    private File outputDirectory;

    @Value("${project.accession:#{null}}")
    private String projectAccession;

    @Value("${project.accession.file:#{null}}")
    private File accessionListFile;

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    @Value("${process.all:false}")
    private boolean processAll;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        if (processAll) {
            prideProjectMongoService.getAllProjectAccessions()
                    .parallelStream().forEach(projAcc -> {
                generateEBeyeXml(projAcc);
            });
        } else if (projectAccession != null) {
            generateEBeyeXml(projectAccession);
        } else if (accessionListFile != null) {
            generateEBeyeXml(accessionListFile);
        }
        log.info("Finished generating EBeye XML.");
        return RepeatStatus.FINISHED;
    }

    private void generateEBeyeXml(File accessionListFile) {
        List<String> accessions = new ArrayList<>();
        try (Stream<String> lines = Files.lines(Paths.get(accessionListFile.getPath()))) {
            accessions.addAll(lines.collect(Collectors.toList()));
        } catch (Exception exception) {
            log.error("Exception in reading accession list file");
        }
        accessions.parallelStream().forEach(accession -> generateEBeyeXml(accession));
    }

    /**
     * This method looks up a project from the provided accession number, and if it is not partial, i.e. with protein IDs,
     * then these are mapped to Uniprot and Ensembl. EBeye XML is then generated for this project and saved to an output file
     * in the PRIDE Archive's EBeye XML directory.
     *
     * @param projectAcc the project's accession number to generate EBeye XML for
     * @throws Exception any problem during the EBeye generation process
     */
    public void generateEBeyeXml(String projectAcc) {
        MongoPrideProject mongoPrideProject = prideProjectMongoService.findByAccession(projectAcc).get();
        Assert.notNull(mongoPrideProject, "Project to update cannot be null! Accession: " + projectAccession);
        File submissionFile = new File(PrideFilePathUtility.getSubmissionFilePath(mongoPrideProject, prideRepoRootPath));
        Map<String, String> proteins = restoreFromFile(projectAcc);
        GenerateEBeyeXMLNew generateEBeyeXMLNew = null;
        try {
            generateEBeyeXMLNew = new GenerateEBeyeXMLNew(mongoPrideProject,
                    SubmissionFileParser.parse(submissionFile), outputDirectory, proteins, true);
            generateEBeyeXMLNew.generate();
        } catch (Exception e) {
            log.info(projectAccession + " : SubmissionFileException or Exception in generating xml");
        }
    }


    public Map<String, String> restoreFromFile(String projectAccession) {
        backupPath = backupPath.endsWith(File.separator) ? backupPath : backupPath + File.separator;
        String dir = backupPath + projectAccession;
        Set<String> proteinAccessions = new HashSet<>();
        Map<String, String> mappedAccessions = new HashMap<>();
        try {
            for (Path f : Files.newDirectoryStream(Paths.get(dir), path -> path.toFile().isFile())) {
                if (f.getFileName().toString().endsWith(PrideMongoPeptideEvidence.class.getSimpleName() + BackupUtil.JSON_EXT)) {
                    List<PrideMongoPeptideEvidence> objs = BackupUtil
                            .getObjectsFromFile(f, PrideMongoPeptideEvidence.class);
                    objs.forEach(o -> {
                        proteinAccessions.add(o.getProteinAccession());
                    });
                }
            }
        } catch (Exception e) {
            log.info(projectAccession + " does not contain PeptideEvidence file");
        }

        proteinAccessions.parallelStream().forEach(accession -> {
            ProteinDetailFetcher.AccessionType type = ProteinDetailFetcher.getAccessionType(accession);
            if (type == ProteinDetailFetcher.AccessionType.UNIPROT_ACC ||
                    type == ProteinDetailFetcher.AccessionType.UNIPROT_ID)
                mappedAccessions.put(accession, "uniprot");

            if (type == ProteinDetailFetcher.AccessionType.ENSEMBL)
                mappedAccessions.put(accession, "ensembl");
        });

        return mappedAccessions;
    }

    public void setBackupPath(String backupPath) {
        this.backupPath = backupPath;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(prideProjectMongoService, "prideProjectMongoService should not be null");
        Assert.notNull(outputDirectory, "Output directory cannot be null.");
    }
}

