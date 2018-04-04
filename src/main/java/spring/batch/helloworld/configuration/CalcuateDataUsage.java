package spring.batch.helloworld.configuration;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

@Configuration
public class CalcuateDataUsage {

  // todo javadoc
  // todo unit tests
  // todo crontab schedule
  // todo production deployment
  // todo CI
  // todo automate deployment

  private final Logger log = LoggerFactory.getLogger(CalcuateDataUsage.class);

  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Value("${pride.archive.data.path}")
  private String prideDataPath;

  @Value("${pride.archive.usage.path}")
  private String prideDataUsageReportPath;

  @Value("${pride.archive.usage.resource}")
  private String subDivision;

  @Value("${pride.archive.usage.trackname}")
  private String trackName;

  private Map<String, Long> dataUsage = new HashMap<>();

  @Bean
  public Step calculateAllDataUsage() {
	return stepBuilderFactory.get("calculateDirectories")
		.tasklet((contribution, chunkContext) -> {
		  log.info("Starting to calculate data usage in PRIDE directory: " + prideDataPath);
		  File parentDataDir = new File(prideDataPath);
		  parsePublicProjects(parentDataDir);
		  parsePrivateProjects(parentDataDir);
		  parseResubProjects(parentDataDir);
		  return RepeatStatus.FINISHED;
		}).build();
  }

  private void parsePublicProjects(File parentDataDir) {
	log.info("Calculating public project data usage.");
	File[] publicDirectoriesYears = parentDataDir.listFiles((dir, name) -> name.matches("^[0-9]{4}$"));
	if (publicDirectoriesYears != null) {
	  for (File year : publicDirectoriesYears) {
		calculatePublicDataDirectory(year);
	  }
	}
  }

  private void calculatePublicDataDirectory(File year) {
	File[] months = year.listFiles();
	if (months != null) {
	  for (File month : months) {
		long yearMonthDataUsage = 0;
		String key = year.getName() + month.getName();
		File[] projects = month.listFiles();
		if (projects != null && 0<projects.length) {
		  for (File project : projects) {
			long projectSize = FileUtils.sizeOfDirectory(project);
			log.info("Project: " + project.getPath() + " Size: " + projectSize);
			yearMonthDataUsage += projectSize;
		  }
		} else {
		  log.error("Public project directory is empty");
		}
		incrementDataUsage(key, yearMonthDataUsage);
	  }
	} else {
	  log.error("Public year directory is empty! " + year.getPath());
	}
  }

  private void parsePrivateProjects(File parentDataDir) {
	log.info("Calculating private project data usage.");
	File[] privateAndValidatedDirectories = parentDataDir.listFiles((dir, name) -> !name.matches("^[0-9]{4}$") && !name.matches("resub"));
	if (privateAndValidatedDirectories != null) {
	  for (File privateOrValidatedDirectory : privateAndValidatedDirectories) {
		calculatePrivateDataDirectory(privateOrValidatedDirectory);
	  }
	}
  }

  private void parseResubProjects(File parentDataDir) {
	log.info("Calculating pending resubmissions project data usage.");
	File[] resubDirectory = parentDataDir.listFiles((dir, name) -> name.matches("resub"));
	if (resubDirectory != null && resubDirectory.length==1) {
	  File[] resubmissions = resubDirectory[0].listFiles();
	  if (resubmissions != null && 0<resubmissions.length) {
		for (File resubmission : resubmissions) {
		  calculatePrivateDataDirectory(resubmission);
		}
	  } else {
		log.error("Unable to find resubmissions directory in: " + parentDataDir);
	  }
	}
  }

  private void calculatePrivateDataDirectory(File privateOrValidatedDirectory) {
	ZonedDateTime earliest = ZonedDateTime.now(ZoneId.systemDefault());
	File[] submissionFile = privateOrValidatedDirectory.listFiles((dir, name) -> name.contentEquals("submission.px")); // pre-validated directory
	if (submissionFile == null || submissionFile.length<1) { // validated directory
	  earliest = getEarliestZonedTimeInSubDirectory(privateOrValidatedDirectory, "internal", earliest);
	  earliest = getEarliestZonedTimeInSubDirectory(privateOrValidatedDirectory, "submitted", earliest);
	} else {
	  earliest = getEarliestZonedDateFromDirectory(privateOrValidatedDirectory);
	}
	int month = earliest.getMonthValue();
	long dataUsage = FileUtils.sizeOfDirectory(privateOrValidatedDirectory);
	log.info("Project: " + privateOrValidatedDirectory.getName() + " " + dataUsage);
	incrementDataUsage( earliest.getYear() + (month<10 ? "0" : "") + month, dataUsage);
  }

  private void incrementDataUsage(String yearMonthKey, long dataUsage) {
	if (this.dataUsage.containsKey(yearMonthKey)) {
	  dataUsage +=  this.dataUsage.get(yearMonthKey);
	}
	this.dataUsage.put(yearMonthKey, dataUsage);
	log.info("Updated data usage: " + yearMonthKey + " " + dataUsage);
  }

  private ZonedDateTime getEarliestZonedTimeInSubDirectory(File privateOrValidatedDirectory, String subDirectory, ZonedDateTime earliestToTest) {
	ZonedDateTime earliest = earliestToTest;
	File[] subDirectoryFiles = privateOrValidatedDirectory.listFiles((dir, name) -> name.contentEquals(subDirectory));
	if (subDirectoryFiles != null && subDirectoryFiles.length==1) {
	  ZonedDateTime earliestSubDirectoryFile = getEarliestZonedDateFromDirectory(subDirectoryFiles[0]);
	  if (earliest.isAfter(earliestSubDirectoryFile)) {
		earliest = earliestSubDirectoryFile;
	  }
	} else {
	  log.error("Unable to find directory files for: " + privateOrValidatedDirectory.getPath() + File.separator + subDirectory);
	}
	return earliest;
  }

  private ZonedDateTime getEarliestZonedDateFromDirectory(File directory) {
	ZonedDateTime earliest = ZonedDateTime.now(ZoneId.systemDefault());
	File[] directoryFiles = directory.listFiles();
	if (directoryFiles != null && 0<directoryFiles.length) {
	  for (File directoryFile : directoryFiles) {
		try {
		  BasicFileAttributes basicFileAttributes = Files.readAttributes(directoryFile.toPath(), BasicFileAttributes.class);
		  ZonedDateTime creation = basicFileAttributes.creationTime().toInstant().atZone(ZoneId.systemDefault());
		  ZonedDateTime modified = basicFileAttributes.lastModifiedTime().toInstant().atZone(ZoneId.systemDefault());
		  if (earliest.isAfter(creation)) {
			earliest = creation;
		  }
		  if (earliest.isAfter(modified)) {
			earliest = modified;
		  }
		} catch (IOException e) {
		  log.error("Unable to read file attributes for: " + directoryFile.getPath(), e);
		}
	  }
	} else {
	  log.error("No files contained in directory: " + directory.getPath());
	}
	return earliest;
  }

  @Bean
  public Step collateAndOutputDataUsage() {
	return stepBuilderFactory.get("collageAndOutputDataUsage")
		.tasklet((contribution, chunkContext) -> {
		  log.info("Collating data usage");
		  YearMonth thisMonth = YearMonth.now();
		  YearMonth finalMonth = thisMonth.minusMonths(1);
		  YearMonth prideStart = YearMonth.of(2005, 1);
		  YearMonth nextMonth = prideStart.plusMonths(0);
		  DateTimeFormatter yearMonthFormatter = DateTimeFormatter.ofPattern("yyyyMM");
		  long runningTotal = 0;
		  StringBuilder reportContent = new StringBuilder();
		  reportContent.append("Date\tbytes\n");
		  for (int i=0; !nextMonth.equals(finalMonth); i++) {
			nextMonth = prideStart.plusMonths(i);
			String yearMonthKey = nextMonth.format(yearMonthFormatter);
			runningTotal += dataUsage.containsKey(yearMonthKey) ? dataUsage.get(yearMonthKey) : 0;
			log.info("Collated usage: " + yearMonthKey + " " + runningTotal);
			reportContent.append(yearMonthKey);
			reportContent.append("\t");
			reportContent.append(runningTotal);
			reportContent.append("\n");
		  }
		  File outputFile = new File(prideDataUsageReportPath + File.separator +
			  finalMonth.format(yearMonthFormatter) + "_" + subDivision + "_" + trackName + ".txt");
		  log.info("Writing report file: " + outputFile.getPath());
		  Files.write(outputFile.toPath(), reportContent.toString().getBytes());
		  return RepeatStatus.FINISHED;
		}).build();
  }

  @Bean
  public Job calculatePrideArchiveDataUsage() {
	return jobBuilderFactory.get("calculatePrideArchiveDataUsage")
		.start(calculateAllDataUsage())
		.next(collateAndOutputDataUsage())
		.build();
  }

}
