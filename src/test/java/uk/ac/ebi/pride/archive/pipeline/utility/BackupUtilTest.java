package uk.ac.ebi.pride.archive.pipeline.utility;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.pride.archive.pipeline.configuration.JobRunnerTestConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.stats.PrideArchiveSubmissionStatsJob;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;
import uk.ac.ebi.pride.mongodb.molecules.model.protein.PrideMongoProteinEvidence;
import uk.ac.ebi.pride.mongodb.molecules.service.molecules.PrideMoleculesMongoService;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Suresh Hewapathirana
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {PrideArchiveSubmissionStatsJob.class, JobRunnerTestConfiguration.class})
@TestPropertySource(value = "classpath:application-test.properties")
@Slf4j
public class BackupUtilTest {

    @Value("${pride.data.backup.path}")
    String backupPath;

    @Autowired
    PrideMoleculesMongoService moleculesService;

    @Test
    public void getPrideMongoProteinEvidenceFromBackupTest() throws IOException {
        String projectAccession = "PXD010142";
        String assayAccession = "93465";
        List<PrideMongoProteinEvidence> prideMongoProteinEvidences = BackupUtil.getPrideMongoProteinEvidenceFromBackup(backupPath, projectAccession, assayAccession);
        prideMongoProteinEvidences.forEach(p -> moleculesService.saveProteinEvidences(p));

        List<PrideMongoPeptideEvidence> prideMongoPeptideEvidences = BackupUtil.getPrideMongoPeptideEvidenceFromBackup(backupPath, projectAccession, assayAccession);
        prideMongoPeptideEvidences.forEach(p -> moleculesService.savePeptideEvidence(p));

        assayAccession = "93466";
        prideMongoProteinEvidences = BackupUtil.getPrideMongoProteinEvidenceFromBackup(backupPath, projectAccession, assayAccession);
        prideMongoProteinEvidences.forEach(p -> moleculesService.saveProteinEvidences(p));

        prideMongoPeptideEvidences = BackupUtil.getPrideMongoPeptideEvidenceFromBackup(backupPath, projectAccession, assayAccession);
        prideMongoPeptideEvidences.forEach(p -> moleculesService.savePeptideEvidence(p));

    }

}