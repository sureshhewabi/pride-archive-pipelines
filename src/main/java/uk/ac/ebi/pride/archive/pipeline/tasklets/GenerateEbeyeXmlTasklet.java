package uk.ac.ebi.pride.archive.pipeline.tasklets;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import uk.ac.ebi.pride.archive.pipeline.utility.BackupUtil;
import uk.ac.ebi.pride.data.io.SubmissionFileParser;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;
import uk.ac.ebi.pride.tools.protein_details_fetcher.ProteinDetailFetcher;
import uk.ac.ebi.pride.tools.protein_details_fetcher.model.Protein;
import uk.ac.ebi.pride.tools.utils.AccessionResolver;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static uk.ac.ebi.pride.archive.pipeline.tasklets.LaunchIndividualEbeyeXmlTasklet.launchIndividualEbeyeXmlGenerationForProjectAcc;
import static uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants.GenerateEbeyeXmlConstants.INTERNAL;
import static uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants.GenerateEbeyeXmlConstants.SUBMISSION_PX;

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

    @Value("${project.accession:''}")
    private String projectAccession;

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    @Value("${process.all:false}")
    private boolean processAll;

    private List<String> exceptionsLaunchingProjects;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        exceptionsLaunchingProjects = Collections.synchronizedList(new ArrayList<String>());
        if (processAll) {
            prideProjectMongoService.getAllProjectAccessions()
                    .parallelStream().forEach(projAcc -> {
                        launchIndividualEbeyeXmlGenerationForProjectAcc(projAcc,
                                prideProjectMongoService, exceptionsLaunchingProjects);
            });
            if (!CollectionUtils.isEmpty(exceptionsLaunchingProjects)) {
                exceptionsLaunchingProjects.parallelStream().forEach(s ->
                        log.error("Problems launching EBeye generation for: " + s));
                throw new JobExecutionException("Unable to launch individual EBeye generation jobs");
            }
        } else {
            generateEBeyeXml(projectAccession);
        }
        log.info("Finished generating EBeye XML.");
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
    private void generateEBeyeXml(String projectAcc) throws Exception {
        MongoPrideProject mongoPrideProject = prideProjectMongoService.findByAccession(projectAcc).get();
        Assert.notNull(mongoPrideProject, "Project to update cannot be null! Accession: " + projectAccession);
        File submissionFile = new File(getSubmissionFilePath(mongoPrideProject));
        Map<String, String> proteins = restoreFromFile(projectAcc);
        GenerateEBeyeXMLNew generateEBeyeXMLNew = new GenerateEBeyeXMLNew(mongoPrideProject,
                SubmissionFileParser.parse(submissionFile), outputDirectory, proteins, true);
        generateEBeyeXMLNew.generate();
    }


    public Map<String, String> restoreFromFile(String projectAccession) throws Exception {
        backupPath = backupPath.endsWith(File.separator) ? backupPath : backupPath + File.separator;
        String dir = backupPath + projectAccession;
        Set<String> proteinAccessions = new HashSet<>();
        Map<String, String> mappedAccessions = new HashMap<>();
        for (Path f : Files.newDirectoryStream(Paths.get(dir), path -> path.toFile().isFile())) {
            if (f.getFileName().toString().endsWith(PrideMongoPeptideEvidence.class.getSimpleName() + BackupUtil.JSON_EXT)) {
                List<PrideMongoPeptideEvidence> objs = BackupUtil
                        .getObjectsFromFile(f, PrideMongoPeptideEvidence.class);
                objs.forEach(o -> {
                    proteinAccessions.add(o.getProteinAccession());
                });
            }
        }

        proteinAccessions.parallelStream().forEach(accession -> {
            ProteinDetailFetcher.AccessionType type = ProteinDetailFetcher.getAccessionType(accession);
            if(type == ProteinDetailFetcher.AccessionType.UNIPROT_ACC ||
                    type == ProteinDetailFetcher.AccessionType.UNIPROT_ID)
                mappedAccessions.put(accession, "uniprot");

            if(type == ProteinDetailFetcher.AccessionType.ENSEMBL)
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

    public String getSubmissionFilePath(MongoPrideProject prideProject) {
        log.info("Generating public file path fragment based on the publication date and project accession");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(prideProject.getPublicationDate());
        int month = calendar.get(Calendar.MONTH) + 1; // the month are zero based, hence the correction +1
        int year = calendar.get(Calendar.YEAR);
        String datePath = year + File.separator + (month < 10 ? "0" : "") + month;
        String publicPath = datePath + File.separator + prideProject.getAccession();
        log.info("Generated public path fragment: " + publicPath);
        prideRepoRootPath = prideRepoRootPath.endsWith(File.separator) ? prideRepoRootPath : prideRepoRootPath + File.separator;
        return prideRepoRootPath + publicPath + File.separator + INTERNAL + File.separator + SUBMISSION_PX;
    }
}

