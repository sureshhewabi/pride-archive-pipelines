package uk.ac.ebi.pride.archive.pipeline.jobs.stats;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Calculates and collates PRIDE Archive data usage for year and month according the TSC and store that into a File in the filesystem
 *
 * specification https://docs.google.com/document/d/1sr90-rsPYk-3uWtMAPNMFBXqMzRtLG0PNpb6--FFXLA/
 */

@Configuration
@Slf4j
@EnableBatchProcessing
public class PrideArchiveDataUsageReportJob extends AbstractArchiveJob {

  @Value("${pride.archive.data.path}")
  private String prideDataPath;

  @Value("${pride.archive.usage.path}")
  private String prideDataUsageReportPath;

  @Value("${pride.archive.usage.resource}")
  private String subDivision;

  @Value("${pride.archive.usage.trackname}")
  private String trackName;

  // The data usage Map keeps the information for all types of data private , public , re-submission, etc.
  private Map<String, Long> dataUsage = new HashMap<>();

  /**
   * Calculates the data usage of public, private, validated, or pre-validated 'resubmission'
   * projects.
   *
   * @return the calculateDirectories step.
   */
  @Bean
  public Step calculateAllDataUsage() {
    return stepBuilderFactory
        .get("calculateAllDataUsage")
        .tasklet(
            (contribution, chunkContext) -> {
              log.info("Starting to calculate data usage in PRIDE directory: " + prideDataPath);
              File parentDataDir = new File(prideDataPath);
              calculateDataUsagePublicProjects(parentDataDir);
              calculateDataUsagePrivateProjects(parentDataDir);
              calculateDataUsageResubProjects(parentDataDir);
              return RepeatStatus.FINISHED;
            })
        .build();
  }

  /**
   * Calculates the data usage of public projects. Taking the root folder for projects in production in PRIDE
   * /nfs/pride/prod/ . List all the folders by filter by year 2011 , 2012 , ...
   *
   * Note: All the other folders are not use for private data.
   *
   * @param parentDataDir the PRIDE Archive parent data directory
   */
  private void calculateDataUsagePublicProjects(File parentDataDir) {
    log.info("Calculating public project data usage.");
    File[] publicDirectoriesYears =
        parentDataDir.listFiles((dir, name) -> name.matches("^[0-9]{4}$"));
    if (publicDirectoriesYears != null) {
      for (File year : publicDirectoriesYears) {
        if(year!=null && year.isDirectory()) {
          calculateDataUsagePublicYearDirectory(year);
        }else{
          log.warn("Year not a directory:"+year);
        }
      }
    }
  }

  /**
   * Calculates the data usage of public data directories from a certain year.
   *
   * @param year the public year directory
   */
  private void calculateDataUsagePublicYearDirectory(File year) {
    File[] months = year.listFiles();
    if (months != null && 0 < months.length) {
      for (File month : months) {
        long yearMonthDataUsage = 0;
        String key = year.getName() + month.getName();
        if(month.isDirectory()){
          File[] projects = month.listFiles();
          if (projects != null && 0 < projects.length) {
            for (File project : projects) {
              if(project.isDirectory()) {
                long projectSize = 0;
                try {
                  projectSize = FileUtils.sizeOfDirectory(project);
                  log.info("Project: " + project.getPath() + " Size: " + projectSize);
                }catch (Exception ex){
                  log.warn("Project: " + project.getPath() + " Size: NOT CALCULATED" );
                }
                yearMonthDataUsage += projectSize;
              }else{
                log.warn("Project not a directory:"+project);
              }
            }
          } else {
            log.error("Public project directory is empty");
          }
          incrementDataUsage(key, yearMonthDataUsage);
        }else{
          log.warn("Month not a directory:"+month);
        }
      }
    } else {
      log.error("Public year directory is empty! " + year.getPath());
    }
  }

  /**
   * plugin Calculates the data usage of private (submitted private, or simply validated) projects
   *
   * @param parentDataDir the PRIDE Archive parent data directory
   */
  private void calculateDataUsagePrivateProjects(File parentDataDir) throws IOException {
    log.info("Calculating private project data usage.");
    File[] privateAndValidatedDirectories =
        parentDataDir.listFiles(
            (dir, name) -> !name.matches("^[0-9]{4}$") && !name.matches("resub") && !name.matches("bin"));
    if (privateAndValidatedDirectories != null) {
      for (File privateOrValidatedDirectory : privateAndValidatedDirectories) {
        calculateDataUsagePrivateValidatedPrevalidatedDirectory(privateOrValidatedDirectory);
      }
    }
  }

  /**
   * Calculates the data usage for 'resubmission' pre-validated project submissions. These folders start with the prefix
   * resub in the main archive prod folder.
   *
   * @param parentDataDir the PRIDE Archive parent data directory
   */
  private void calculateDataUsageResubProjects(File parentDataDir) throws IOException {
    log.info("Calculating pending resubmissions project data usage.");
    File[] resubDirectory = parentDataDir.listFiles((dir, name) -> name.matches("resub"));
    if (resubDirectory != null && resubDirectory.length == 1 && resubDirectory[0].isDirectory()) {
      File[] resubmissions = resubDirectory[0].listFiles();
      if (resubmissions != null && 0 < resubmissions.length) {
        for (File resubmission : resubmissions) {
          calculateDataUsagePrivateValidatedPrevalidatedDirectory(resubmission);
        }
      } else {
        log.error("Unable to find resubmissions directory in: " + parentDataDir);
      }
    }else{
      log.warn("calculateDataUsageResubProjects: resub directory not found");
    }
  }

  /**
   * Calculates the data usage of a private, validated, or pre-validated directory
   *
   * @param directory the input directory to calculate the file size for.
   */
  private void calculateDataUsagePrivateValidatedPrevalidatedDirectory(File directory) throws IOException {
    try{
      if(directory.exists() && directory.isDirectory()) {
        ZonedDateTime earliest = ZonedDateTime.now(ZoneId.systemDefault());
        File[] submissionFile = directory.listFiles((dir, name) -> name.contentEquals("submission.px")); // pre-validated directory
        if (submissionFile == null || submissionFile.length < 1) { // validated directory
          earliest = getEarliestZonedTimeInSubDirectory(directory, "internal", earliest);
          earliest = getEarliestZonedTimeInSubDirectory(directory, "submitted", earliest);
        } else {
          earliest = getEarliestZonedDateFromDirectory(directory);
        }
        int month = earliest.getMonthValue();
        long dataUsage = FileUtils.sizeOfDirectory(directory);
        log.info("Project: " + directory.getName() + " " + dataUsage);
        incrementDataUsage(earliest.getYear() + (month < 10 ? "0" : "") + month, dataUsage);
      }else{
        log.warn("calculateDataUsagePrivateValidatedPrevalidatedDirectory: not a dir or doesn't exists : "+directory);
      }
    }catch(Exception e){
      log.error("Error in calculateDataUsagePrivateValidatedPrevalidatedDirectory:"+e.getMessage(),e);
    }
  }

  /**
   * Increments the data usage for the YYYYMM key.
   *
   * @param yearMonthKey the YYYYMM key
   * @param dataUsage the amount to increment by.
   */
  private void incrementDataUsage(String yearMonthKey, long dataUsage) {
    if (this.dataUsage.containsKey(yearMonthKey)) {
      dataUsage += this.dataUsage.get(yearMonthKey);
    }
    this.dataUsage.put(yearMonthKey, dataUsage);
    log.info("Updated data usage: " + yearMonthKey + " " + dataUsage);
  }

  /**
   * Gets the earliest ZonedDateTime of files within a sub-directory.
   *
   * @param privateOrValidatedDirectory a private or validated directory
   * @param subDirectory the sub-directory name
   * @param earliestToTest the earliest ZonedDateTime to test against initially
   * @return the earliest ZonedDateTime of a file, default being the input earliestToTest
   */
  private ZonedDateTime getEarliestZonedTimeInSubDirectory(File privateOrValidatedDirectory, String subDirectory, ZonedDateTime earliestToTest) throws IOException {
    ZonedDateTime earliest = earliestToTest;
    File[] subDirectoryFiles = privateOrValidatedDirectory.listFiles((dir, name) -> name.contentEquals(subDirectory));
    if (subDirectoryFiles != null && subDirectoryFiles.length == 1) {
      ZonedDateTime earliestSubDirectoryFile =
          getEarliestZonedDateFromDirectory(subDirectoryFiles[0]);
      if (earliest.isAfter(earliestSubDirectoryFile)) {
        earliest = earliestSubDirectoryFile;
      }
    } else {
      log.error("Unable to find directory files for: " + privateOrValidatedDirectory.getPath() + File.separator + subDirectory);
    }
    return earliest;
  }

  /**
   * Gets the earliest ZonedDateTime of files within a directory.
   *
   * @param directory the parent directory to asses files within
   * @return the earliest ZonedDateTime of a file, default being the input earliestToTest
   */
  private ZonedDateTime getEarliestZonedDateFromDirectory(File directory) throws IOException {
    ZonedDateTime earliest = ZonedDateTime.now(ZoneId.systemDefault());
    File[] directoryFiles = directory.listFiles();
    if (directoryFiles != null && 0 < directoryFiles.length) {
      for (File directoryFile : directoryFiles) {
        BasicFileAttributes basicFileAttributes =
            Files.readAttributes(directoryFile.toPath(), BasicFileAttributes.class);
        ZonedDateTime creation =
            basicFileAttributes.creationTime().toInstant().atZone(ZoneId.systemDefault());
        ZonedDateTime modified =
            basicFileAttributes.lastModifiedTime().toInstant().atZone(ZoneId.systemDefault());
        if (earliest.isAfter(creation)) {
          earliest = creation;
        }
        if (earliest.isAfter(modified)) {
          earliest = modified;
        }
      }
    } else {
      log.error("No files contained in directory: " + directory.getPath());
    }
    return earliest;
  }

  /**
   * Collates the data usage according to year and month, and then outputs it to a file according to
   * the TSC spec.
   *
   * @return the collageAndOutputDataUsage step
   */
  @Bean
  public Step collateAndOutputDataUsage() {
    return stepBuilderFactory
        .get("collateAndOutputDataUsage")
        .tasklet(
            (contribution, chunkContext) -> {
              log.info("Collating data usage");
              YearMonth thisMonth = YearMonth.now();
              YearMonth finalMonth = thisMonth.minusMonths(1);
              YearMonth prideStart = YearMonth.of(2005, 1);
              YearMonth nextMonth = prideStart.plusMonths(0);
              DateTimeFormatter yearMonthFormatter = DateTimeFormatter.ofPattern("yyyyMM");
              long runningTotal = 0;
              StringBuilder reportContent = new StringBuilder();
              reportContent.append("Date\tbytes\n");
              for (int i = 0; !nextMonth.equals(finalMonth); i++) {
                nextMonth = prideStart.plusMonths(i);
                String yearMonthKey = nextMonth.format(yearMonthFormatter);
                runningTotal +=
                    dataUsage.containsKey(yearMonthKey) ? dataUsage.get(yearMonthKey) : 0;
                log.info("Collated usage: " + yearMonthKey + " " + runningTotal);
                reportContent.append(yearMonthKey);
                reportContent.append("\t");
                reportContent.append(runningTotal);
                reportContent.append("\n");
              }
              File outputFile =
                  new File(
                      prideDataUsageReportPath
                          + File.separator
                          + finalMonth.format(yearMonthFormatter)
                          + "_"
                          + subDivision
                          + "_"
                          + trackName
                          + ".txt");
              log.info("Writing report file: " + outputFile.getPath());
              Files.write(outputFile.toPath(), reportContent.toString().getBytes());
              return RepeatStatus.FINISHED;
            })
        .build();
  }

  /**
   * Defines the job to calculate and collate PRIDE Archive data usage.
   *
   * @return the calculatePrideArchiveDataUsage job
   */
  @Bean
  public Job calculatePrideArchiveDataUsage() {
    return jobBuilderFactory
        .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_DATA_USAGE.getName())
        .start(calculateAllDataUsage())
        .next(collateAndOutputDataUsage())
        .build();
  }

  /**
   * Sets new prideDataPath.
   *
   * @param prideDataPath New value of prideDataPath.
   */
  public void setPrideDataPath(String prideDataPath) {
    this.prideDataPath = prideDataPath;
  }

  /**
   * Gets prideDataUsageReportPath.
   *
   * @return Value of prideDataUsageReportPath.
   */
  public String getPrideDataUsageReportPath() {
    return prideDataUsageReportPath;
  }

  /**
   * Sets new prideDataUsageReportPath.
   *
   * @param prideDataUsageReportPath New value of prideDataUsageReportPath.
   */
  public void setPrideDataUsageReportPath(String prideDataUsageReportPath) {
    this.prideDataUsageReportPath = prideDataUsageReportPath;
  }
}
