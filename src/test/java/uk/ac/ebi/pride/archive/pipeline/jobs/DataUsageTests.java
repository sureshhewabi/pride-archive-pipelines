package uk.ac.ebi.pride.archive.pipeline.jobs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StringUtils;
import uk.ac.ebi.pride.archive.pipeline.configuration.JobRunnerConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.stats.PrideArchiveDataUsageJob;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Tests the PRIDE Archive Data Usage job.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@EnableConfigurationProperties
@ContextConfiguration(classes = {PrideArchiveDataUsageJob.class, JobRunnerConfiguration.class})
@TestPropertySource(locations = "classpath:application-test.properties")
@Slf4j
public class DataUsageTests {

  @Rule public TemporaryFolder tempDatafolder = new TemporaryFolder();
  @Rule public TemporaryFolder tempReportFolder = new TemporaryFolder();

  @Autowired
  PrideArchiveDataUsageJob prideArchiveDataUsage;

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  /**
   * Tests calculating and collating data usage.
   *
   * @throws Exception Failures calculating and collating data usage.
   */
  @Test
  public void calculateAndCollateDataUsage() throws Exception {
    setupTempDataDirectoriesAndFiles();

    JobExecution jobExecution = jobLauncherTestUtils.launchJob();
    Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());

    validateReportOutputFile();
    calcAgainUsingEmptyDirectories();
  }

  /**
   * Tests the calculation step only, using a combination of different empty data directories.
   *
   * @throws IOException Failures reading data files.
   */
  private void calcAgainUsingEmptyDirectories() throws IOException {
    JobExecution jobExecution;
    TemporaryFolder secondaryDataFolder = new TemporaryFolder();
    secondaryDataFolder.create();
    prideArchiveDataUsage.setPrideDataPath(secondaryDataFolder.getRoot().getPath());
    // empty year, separately an empty month
    File emptyYear = secondaryDataFolder.newFolder("2015");
    emptyYear.deleteOnExit();
    File nextYear = secondaryDataFolder.newFolder("2016");
    nextYear.deleteOnExit();
    File emptyMonth = secondaryDataFolder.newFolder(nextYear.getName(), "11");
    emptyMonth.deleteOnExit();
    File emptyResub = secondaryDataFolder.newFolder("resub");
    emptyResub.deleteOnExit();
    File emptyPrivate = secondaryDataFolder.newFolder("1-2018-456");
    emptyPrivate.deleteOnExit();
    jobExecution = jobLauncherTestUtils.launchStep("calculateAllDataUsage");
    Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());

    TemporaryFolder tertiaryDataFolder = new TemporaryFolder();
    tertiaryDataFolder.create();
    prideArchiveDataUsage.setPrideDataPath(tertiaryDataFolder.getRoot().getPath());
    jobExecution = jobLauncherTestUtils.launchStep("calculateAllDataUsage");
    Assert.assertEquals(BatchStatus.COMPLETED.name(), jobExecution.getExitStatus().getExitCode());
  }

  /**
   * Validates the report output file.
   * Todo: validate all lines of file semantically and syntactically not just the last line
   *
   * @throws IOException Failures related to reading the report output file.
   */
  private void validateReportOutputFile() throws IOException {

    File reportDirectory = new File(prideArchiveDataUsage.getPrideDataUsageReportPath());
    File[] reportDirFiles = reportDirectory.listFiles();
    Assert.assertTrue("Report file should exist in output directory", reportDirFiles != null && reportDirFiles.length == 1);
    ReversedLinesFileReader reversedLinesFileReader = new ReversedLinesFileReader(reportDirFiles[0], Charset.forName("UTF-8"));
    String lastLine = reversedLinesFileReader.readLine();
    YearMonth thisMonth = YearMonth.now();
    YearMonth finalMonth = thisMonth.minusMonths(1);
    DateTimeFormatter yearMonthFormatter = DateTimeFormatter.ofPattern("yyyyMM");
    Assert.assertTrue("Report file is not blank and has a last line", 0 < reportDirFiles[0].length() && !StringUtils.isEmpty(lastLine));
    Assert.assertTrue("Report file last line has a single tab character", lastLine.contains("\t"));
    String[] lineParts = lastLine.split("\t");
    Assert.assertEquals("Report file last line has 2 parts", 2, lineParts.length);
    Assert.assertTrue("Final line should contain previous month YYYYMM, tab, final total byte count", finalMonth.format(yearMonthFormatter).equals(lineParts[0]) && 0 < Long.parseLong(lineParts[1]));
  }

  /**
   * Sets up example data directories.
   *
   * @throws Exception Problems with creating temporoary files and directories
   */
  private void setupTempDataDirectoriesAndFiles() throws Exception {
    // public data setup
    tempDatafolder.create();
    tempReportFolder.create();
    File publicYear = tempDatafolder.newFolder("2015");
    File publicMonth = tempDatafolder.newFolder(publicYear.getName(), "10");
    File publicAccession = tempDatafolder.newFolder(publicYear.getName(), publicMonth.getName(), "PXT000001");
    File publicInternal = tempDatafolder.newFolder(publicYear.getName(), publicMonth.getName(), publicAccession.getName(), "internal");
    File publicSubmissionPx = new File(publicInternal.getPath() + File.separator + "submission.px");
    File publicSubmitted = tempDatafolder.newFolder(publicYear.getName(), publicMonth.getName(), publicAccession.getName(), "submitted");
    File publicSubmittedRaw = new File(publicSubmitted.getPath() + File.separator + "foo.raw");
    createTempFileAndSetCreationTime(publicSubmissionPx, "2015-10-20");
    createTempFileAndSetCreationTime(publicSubmittedRaw, "2015-10-10");

    // private data setup
    File privateAccession = tempDatafolder.newFolder("PXT000002");
    File privateInternal = tempDatafolder.newFolder(privateAccession.getName(), "internal");
    File privateSubmissionPx = new File(privateInternal.getPath() + File.separator + "submission.px");
    File privateSubmitted = tempDatafolder.newFolder(privateAccession.getName(), "submitted");
    File privateSubmittedRaw = new File(privateSubmitted.getPath() + File.separator + "bar.raw");
    createTempFileAndSetCreationTime(privateSubmissionPx, "2016-10-20");
    createTempFileAndSetCreationTime(privateSubmittedRaw, "2016-10-10");
    File privateSecondAccession = tempDatafolder.newFolder("PXT000003");
    File privateSecondInternal = tempDatafolder.newFolder(privateSecondAccession.getName(), "internal");
    File privateSecondSubmissionPx = new File(privateSecondInternal.getPath() + File.separator + "submission.px");
    File privateSecondSubmitted = tempDatafolder.newFolder(privateSecondAccession.getName(), "submitted");
    File privateSecondSubmittedRaw = new File(privateSecondSubmitted.getPath() + File.separator + "bar.raw");
    File privateSecondSubmittedSearch = new File(privateSecondSubmitted.getPath() + File.separator + "foo.txt");
    createTempFileAndSetCreationTime(privateSecondSubmissionPx, "2016-10-22");
    createTempFileAndSetCreationTime(privateSecondSubmittedRaw, "2016-10-11");
    setModificationTime(privateSecondSubmittedRaw, "2016-10-05");
    createTempFileAndSetCreationTime(privateSecondSubmittedSearch, "2016-10-04");
    setModificationTime(privateSecondSubmittedSearch, "2016-10-03");

    // validated data setup
    File validatedAccession = tempDatafolder.newFolder("1-20180504-123456");
    File validatedInternal = tempDatafolder.newFolder(validatedAccession.getName(), "internal");
    File validatedSubmissionPx = new File(validatedInternal.getPath() + File.separator + "submission.px");
    File validatedSubmitted = tempDatafolder.newFolder(validatedAccession.getName(), "submitted");
    File validatedSubmittedRaw = new File(validatedSubmitted.getPath() + File.separator + "bar.raw");
    createTempFileAndSetCreationTime(validatedSubmissionPx, "2017-10-20");
    createTempFileAndSetCreationTime(validatedSubmittedRaw, "2017-10-20");
    File validatedAccessionEmptySubDir = tempDatafolder.newFolder("1-20180504-456");
    File validatedInternalEmptyInternalSubDir = tempDatafolder.newFolder(validatedAccessionEmptySubDir.getName(), "internal");
    validatedInternalEmptyInternalSubDir.deleteOnExit();
    File validatedInternalEmptySubmittedSubDir = tempDatafolder.newFolder(validatedAccessionEmptySubDir.getName(), "submitted");
    validatedInternalEmptySubmittedSubDir.deleteOnExit();

    // resub data setup
    File resub = tempDatafolder.newFolder("resub");
    File resubPrevalidAccession = tempDatafolder.newFolder(resub.getName(), "1-20180504-123456");
    File resubPrevalidSubmissionPx = new File(resubPrevalidAccession.getPath() + File.separator + "submission.px");
    File resubPrevalidSubmittedRaw = new File(resubPrevalidAccession.getPath() + File.separator + "bar.raw");
    createTempFileAndSetCreationTime(resubPrevalidSubmissionPx, "2018-01-20");
    createTempFileAndSetCreationTime(resubPrevalidSubmittedRaw, "2018-01-10");

    prideArchiveDataUsage.setPrideDataPath(tempDatafolder.getRoot().getPath());
    prideArchiveDataUsage.setPrideDataUsageReportPath(tempReportFolder.getRoot().getPath());
  }

  /**
   * Creates the temporary file, and setes its creation time attribute.
   *
   * @param tempFile the temporary file to create
   * @param localDateYearMonthDay the creation time to set the file to
   * @throws IOException problems creating the file, or setting the file attribute
   */
  private void createTempFileAndSetCreationTime(File tempFile, String localDateYearMonthDay) throws IOException {
    generateTempFile(tempFile);
    FileTime tempFiletime = FileTime.from(LocalDate.parse(localDateYearMonthDay).atStartOfDay().toInstant(ZoneOffset.UTC));
    Files.setAttribute(tempFile.toPath(), "creationTime", tempFiletime);
  }

  /**
   * Sets the modification time for a (temporary) file.
   *
   * @param tempFile the temporary file to be modified
   * @param localDateYearMonthDay the modification time to set the file to
   * @throws IOException problems setting the file attribute
   */
  private void setModificationTime(File tempFile, String localDateYearMonthDay) throws IOException {
    FileTime tempFiletime = FileTime.from(LocalDate.parse(localDateYearMonthDay).atStartOfDay().toInstant(ZoneOffset.UTC));
    Files.setAttribute(tempFile.toPath(), "lastModifiedTime", tempFiletime);
  }

  /**
   * Generated a new temporary file, with a dummy file size.
   *
   * @param tempFile the temporary file to create with a default 1024 bytes size.
   * @throws IOException problems creating the temporary file
   */
  private void generateTempFile(File tempFile) throws IOException {
    log.info("Created new file file for: " + tempFile.getPath() + " ? " + tempFile.createNewFile());
    tempFile.deleteOnExit();
    RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
    raf.setLength(1024);
  }
}
