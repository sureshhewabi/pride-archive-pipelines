package uk.ac.ebi.pride.archive.pipeline.core.transformers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;
import uk.ac.ebi.pride.archive.repo.client.CvParamRepoClient;
import uk.ac.ebi.pride.archive.repo.client.UserRepoClient;
import uk.ac.ebi.pride.archive.repo.models.param.CvParam;
import uk.ac.ebi.pride.archive.repo.models.project.*;
import uk.ac.ebi.pride.archive.repo.models.user.User;
import uk.ac.ebi.pride.data.model.Submission;
import uk.ac.ebi.pride.pubmed.PubMedFetcher;
import uk.ac.ebi.pride.pubmed.model.EupmcReferenceSummary;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;

/**
 * This class helps to transform the Submission object to Project Object
 */
@Slf4j
public class SubmissionToProjectTransformer {

    UserRepoClient userRepoClient;
    CvParamRepoClient cvParamRepoClient;
    Project modifiedProject;

    public SubmissionToProjectTransformer(CvParamRepoClient cvParamRepoClient,  UserRepoClient userRepoClient) {
        this.cvParamRepoClient = cvParamRepoClient;
        this.userRepoClient = userRepoClient;

    }

    /**
     * Copy submission.px data to the Project Object
     * @param submission Data coming from submission.px file
     * @param modifiedProject Project object with changes
     * @return Project object with changes
     */
    public Project transform(Submission submission, Project modifiedProject) throws IOException {
        this.modifiedProject = modifiedProject;
        modifiedProject.setTitle(submission.getProjectMetaData().getProjectTitle());
        modifiedProject.setProjectDescription(submission.getProjectMetaData().getProjectDescription());
        modifiedProject.setDataProcessingProtocol(submission.getProjectMetaData().getDataProcessingProtocol());
        modifiedProject.setSampleProcessingProtocol(submission.getProjectMetaData().getSampleProcessingProtocol());
        modifiedProject.setKeywords(submission.getProjectMetaData().getKeywords());

        // set submitter (change only if the dataset is private)
        if(!modifiedProject.isPublicProject()){
            final String newEmail = submission.getProjectMetaData().getSubmitterContact().getEmail();
            Optional<User> submitterContact = userRepoClient.findByEmail(newEmail);
            if(submitterContact.isPresent()) {
                User submitterContactUser = submitterContact.get();
                submitterContactUser.setAffiliation(submission.getProjectMetaData().getSubmitterContact().getAffiliation());
                modifiedProject.setSubmitter(submitterContactUser);
            }else{
                log.warn("No user found with email : " + newEmail);
            }
        }else{
            log.warn("Submitter cannot be changed for public datasets!");
        }

        // Set sample CV Params
        Set<ProjectSampleCvParam> projectSampleCvParams = new HashSet<>();
        submission.getProjectMetaData().getSpecies().forEach(speciesCvParam -> {
            projectSampleCvParams.add(cvParamToProjectSampleCvParam(speciesCvParam));
        });
        submission.getProjectMetaData().getTissues().forEach(tissuesCvParam -> {
            projectSampleCvParams.add(cvParamToProjectSampleCvParam(tissuesCvParam));
        });
        submission.getProjectMetaData().getCellTypes().forEach(cellTypesCvParam -> {
            projectSampleCvParams.add(cvParamToProjectSampleCvParam(cellTypesCvParam));
        });
        submission.getProjectMetaData().getDiseases().forEach(diseasesCvParam -> {
            projectSampleCvParams.add(cvParamToProjectSampleCvParam(diseasesCvParam));
        });
        modifiedProject.setSamples(projectSampleCvParams);

        // Set experiment types CV Param
        Set<ProjectExperimentType> projectExperimentTypes = new HashSet<>();
        submission.getProjectMetaData().getMassSpecExperimentMethods().forEach(experimentTypeCvParam -> {
            projectExperimentTypes.add(cvParamToProjectExperimentTypeCvParam(experimentTypeCvParam));
        });
        modifiedProject.setExperimentTypes(projectExperimentTypes);

        // Set Instrument CV Param
        Set<ProjectInstrumentCvParam> projectInstrumentCvParams = new HashSet<>();
        submission.getProjectMetaData().getInstruments().forEach(instrumentCvParam -> {
            projectInstrumentCvParams.add(cvParamToProjectInstrumentCvParam(instrumentCvParam));
        });
        modifiedProject.setInstruments(projectInstrumentCvParams);

        // Set quantification methods
        Set<ProjectQuantificationMethodCvParam> projectQuantificationMethodCvParams = new HashSet<>();
        submission.getProjectMetaData().getQuantifications().forEach(quantificationMethodCvParam -> {
            projectQuantificationMethodCvParams.add(cvParamToProjectQuantificationMethodCvParam(quantificationMethodCvParam));
        });
        modifiedProject.setQuantificationMethods(projectQuantificationMethodCvParams);

        // set modifications
        Set<ProjectPTM> projectPTMS = new HashSet<>();
        submission.getProjectMetaData().getModifications().forEach(cvParam -> {
            projectPTMS.add(cvParamToProjectPTM(cvParam));
        });
        modifiedProject.setPtms(projectPTMS);

        // Set Project Tag
        Set<ProjectTag> projectTags = new HashSet<>();
        submission.getProjectMetaData().getProjectTags().forEach(projectTag -> {
            ProjectTag tag = new ProjectTag();
            tag.setTag(projectTag);
            projectTags.add(tag);
            tag.setProject(modifiedProject);
        });
        modifiedProject.setProjectTags(projectTags);

        // set Other omics links
        modifiedProject.setOtherOmicsLink(submission.getProjectMetaData().getOtherOmicsLink());

        // set Reanalysis
        Set<String> reanalysis = submission.getProjectMetaData().getReanalysisAccessions();
        String reanalysisString = String.join(",", reanalysis);
        modifiedProject.setReanalysis((reanalysisString.equals("")? null : reanalysisString ));

        // Set References
        setReferenceList(submission, modifiedProject);

        return modifiedProject;
    }

    /**
     * Convert CvParam To ProjectSampleCvParam
     * @param submissionCvParam CvParam from the Submission object
     * @return ProjectSampleCvParam object
     */
    private ProjectSampleCvParam cvParamToProjectSampleCvParam(uk.ac.ebi.pride.data.model.CvParam submissionCvParam) {

        ProjectSampleCvParam projectSampleCvParam = new ProjectSampleCvParam();
        CvParam cvParam = new CvParam();

        for (ProjectSampleCvParam param: modifiedProject.getSamples()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectSampleCvParam.setId(param.getId());
                break;
            }
        }

        CvParamCheckExists(submissionCvParam, cvParam);
        cvParam.setAccession(submissionCvParam.getAccession());
        cvParam.setName(submissionCvParam.getName());
        cvParam.setCvLabel(submissionCvParam.getCvLabel());
        projectSampleCvParam.setCvParam(cvParam);
        projectSampleCvParam.setProject(modifiedProject);
        projectSampleCvParam.setValue(cvParam.getValue());
        return projectSampleCvParam;
    }

    /**
     * Convert CvParam To ProjectExperimentType
     * @param submissionCvParam CvParam from the Submission object
     * @return ProjectExperimentType object
     */
    private ProjectExperimentType cvParamToProjectExperimentTypeCvParam(uk.ac.ebi.pride.data.model.CvParam submissionCvParam) {

        ProjectExperimentType projectExperimentType = new ProjectExperimentType();
        CvParam cvParam = new CvParam();

        for (ProjectExperimentType param: modifiedProject.getExperimentTypes()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectExperimentType.setId(param.getId());
                break;
            }
        }

        CvParamCheckExists(submissionCvParam, cvParam);
        cvParam.setAccession(submissionCvParam.getAccession());
        cvParam.setName(submissionCvParam.getName());
        cvParam.setCvLabel(submissionCvParam.getCvLabel());
        projectExperimentType.setCvParam(cvParam);
        projectExperimentType.setProject(modifiedProject);
        projectExperimentType.setValue(cvParam.getValue());
        return projectExperimentType;
    }

    /**
     * Convert CvParam To ProjectInstrumentCvParam
     * @param submissionCvParam CvParam from the Submission object
     * @return projectInstrumentCvParam object
     */
    private ProjectInstrumentCvParam cvParamToProjectInstrumentCvParam(uk.ac.ebi.pride.data.model.CvParam submissionCvParam) {

        ProjectInstrumentCvParam projectInstrumentCvParam = new ProjectInstrumentCvParam();
        CvParam cvParam = new CvParam();

        for (ProjectInstrumentCvParam param: modifiedProject.getInstruments()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectInstrumentCvParam.setId(param.getId());
                break;
            }
        }

        CvParamCheckExists(submissionCvParam, cvParam);
        cvParam.setAccession(submissionCvParam.getAccession());
        cvParam.setName(submissionCvParam.getName());
        cvParam.setCvLabel(submissionCvParam.getCvLabel());
        projectInstrumentCvParam.setCvParam(cvParam);
        projectInstrumentCvParam.setProject(modifiedProject);
        projectInstrumentCvParam.setValue(cvParam.getValue());
        return projectInstrumentCvParam;
    }

    /**
     * Convert CvParam To QuantificationMethodCvParam
     * @param submissionCvParam CvParam from the Submission object
     * @return quantificationMethodCvParam object
     */
    private ProjectQuantificationMethodCvParam cvParamToProjectQuantificationMethodCvParam(uk.ac.ebi.pride.data.model.CvParam submissionCvParam) {

        ProjectQuantificationMethodCvParam projectQuantificationMethodCvParam = new ProjectQuantificationMethodCvParam();
        CvParam cvParam = new CvParam();

        for (ProjectQuantificationMethodCvParam param: modifiedProject.getQuantificationMethods()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectQuantificationMethodCvParam.setId(param.getId());
                break;
            }
        }

        CvParamCheckExists(submissionCvParam, cvParam);
        cvParam.setAccession(submissionCvParam.getAccession());
        cvParam.setName(submissionCvParam.getName());
        cvParam.setCvLabel(submissionCvParam.getCvLabel());
        projectQuantificationMethodCvParam.setCvParam(cvParam);
        projectQuantificationMethodCvParam.setProject(modifiedProject);
        projectQuantificationMethodCvParam.setValue(cvParam.getValue());
        return projectQuantificationMethodCvParam;
    }

    /**
     * Convert CvParam To ProjectPTM
     * @param submissionCvParam CvParam from the Submission object
     * @return projectPTM object
     */
    private ProjectPTM cvParamToProjectPTM(uk.ac.ebi.pride.data.model.CvParam submissionCvParam) {

        ProjectPTM projectPTM = new ProjectPTM();
        CvParam cvParam = new CvParam();

        for (ProjectPTM param: modifiedProject.getPtms()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectPTM.setId(param.getId());
                break;
            }
        }

        CvParamCheckExists(submissionCvParam, cvParam);
        cvParam.setAccession(submissionCvParam.getAccession());
        cvParam.setName(submissionCvParam.getName());
        cvParam.setCvLabel(submissionCvParam.getCvLabel());
        projectPTM.setCvParam(cvParam);
        projectPTM.setProject(modifiedProject);
        projectPTM.setValue(cvParam.getValue());
        return projectPTM;
    }

    /**
     * Check if the Cv Param is already in the database. If it is already exists, use the id of the CV param
     * to update the record. If not, leave id empty to insert a new one
     * @param submissionCvParam
     * @param cvParam
     */
    private void CvParamCheckExists(uk.ac.ebi.pride.data.model.CvParam submissionCvParam, CvParam cvParam) {
        try {
            CvParam cvParamExits =  cvParamRepoClient.findByAccession(submissionCvParam.getAccession());
            if(!submissionCvParam.getName().toLowerCase().trim().equals(cvParamExits.getName().toLowerCase().trim())){
                log.warn("CV term ("+ submissionCvParam.getAccession()+") mismatch found! " + submissionCvParam.getName()
                        + " is reported as "
                        + cvParamExits.getName() + " in the database");
            }
            if(cvParamExits.getId() !=  null){
                cvParam.setId(cvParamExits.getId());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method reads doi or/and pubmedID, extract the reference line from EPMC,
     * and update the references
     * @param submission input data as submission
     * @param modifiedProject project object to be updated the references
     */
    private void setReferenceList(Submission submission, Project modifiedProject){

        Set<String> pubmedIds = submission.getProjectMetaData().getPubmedIds();
        Set<String> dois = submission.getProjectMetaData().getDois();

        modifiedProject.setReferences(new LinkedList<>());

        for (String pubMedId:pubmedIds) {

            Reference reference = new Reference();
            EupmcReferenceSummary eupmcReferenceSummary = null;
            try {
                eupmcReferenceSummary = PubMedFetcher.getPubMedSummary(pubMedId);
            } catch (URISyntaxException | IOException e) {
                e.printStackTrace();
            }
            String referenceLine = eupmcReferenceSummary.getRefLine();
            Assert.hasText(referenceLine, "No reference added: Could not retrieve reference line with PUBMED: " + pubMedId);
            reference.setReferenceLine(referenceLine);
            reference.setPubmedId(Integer.parseInt(pubMedId));
            reference.setDoi(eupmcReferenceSummary.getEupmcResult().getDoi());
            log.info("Adding reference to PRIDE Archive project: (" + pubMedId + ") " + referenceLine);
            reference.setProject(modifiedProject);
            modifiedProject.getReferences().add(reference);
        }

        for (String doi:dois) {

            Reference reference = new Reference();
            if (StringUtils.isNotEmpty(doi)) { // DOI reference
                String doiUppercase = doi.toUpperCase();
                if (doiUppercase.contains("HTTP") || doiUppercase.contains("DOI.ORG")) { // strip protocol and domain
                    doi = doiUppercase.replaceFirst("((HTTP|HTTPS)://)?(DX\\.)?DOI\\.ORG/", "");
                } else if(doiUppercase.contains("DOI:")) { // strip protocol
                    doi = doiUppercase.replaceFirst("DOI:", "");
                } else {
                    doi = doiUppercase;
                }
                reference.setDoi(doi);
                log.info("Adding reference to PRIDE project: (" + doi + ").");
                reference.setProject(modifiedProject);
                modifiedProject.getReferences().add(reference);
            }
        }
    }
}
