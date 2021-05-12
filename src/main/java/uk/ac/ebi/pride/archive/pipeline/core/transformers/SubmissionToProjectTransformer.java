package uk.ac.ebi.pride.archive.pipeline.core.transformers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;
import uk.ac.ebi.pride.archive.dataprovider.utils.TitleConstants;
import uk.ac.ebi.pride.archive.repo.client.CvParamRepoClient;
import uk.ac.ebi.pride.archive.repo.client.UserRepoClient;
import uk.ac.ebi.pride.archive.repo.models.param.CvParam;
import uk.ac.ebi.pride.archive.repo.models.project.*;
import uk.ac.ebi.pride.archive.repo.models.user.User;
import uk.ac.ebi.pride.data.model.Contact;
import uk.ac.ebi.pride.data.model.Submission;
import uk.ac.ebi.pride.pubmed.PubMedFetcher;
import uk.ac.ebi.pride.pubmed.model.EupmcReferenceSummary;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * This class helps to transform the Submission object to Project Object
 */
@Slf4j
public class SubmissionToProjectTransformer {

    UserRepoClient userRepoClient;
    CvParamRepoClient cvParamRepoClient;
    Project modifiedProject;
    List<CvParam> cvParamList;

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
        this.cvParamList = cvParamRepoClient.findAll();
        modifiedProject.setTitle(submission.getProjectMetaData().getProjectTitle());
        modifiedProject.setProjectDescription(submission.getProjectMetaData().getProjectDescription());
        modifiedProject.setDataProcessingProtocol(submission.getProjectMetaData().getDataProcessingProtocol());
        modifiedProject.setSampleProcessingProtocol(submission.getProjectMetaData().getSampleProcessingProtocol());
        modifiedProject.setKeywords(submission.getProjectMetaData().getKeywords());

        // set submitter and labhead (change only if the dataset is private)
        if(!modifiedProject.isPublicProject()){

            // update submitter
            final String newEmail = submission.getProjectMetaData().getSubmitterContact().getEmail();
            Optional<User> submitterContact = userRepoClient.findByEmail(newEmail);
            if(submitterContact.isPresent()) {
                User submitterContactUser = submitterContact.get();
                submitterContactUser.setAffiliation(submission.getProjectMetaData().getSubmitterContact().getAffiliation());
                modifiedProject.setSubmitter(submitterContactUser);
            }else{
                log.warn("No user found with email : " + newEmail);
            }

            // update Labhead
            Contact labHeadContact = submission.getProjectMetaData().getLabHeadContact();

            if (labHeadContact.getName() != null &&
                    labHeadContact.getEmail() != null &&
                    labHeadContact.getAffiliation() != null) {

                LabHead labHead = convertLabHead(labHeadContact);
                List<LabHead> labHeads = new ArrayList<>();
                labHeads.add(labHead);
                modifiedProject.setLabHeads(labHeads);
            }else{
                log.warn("Missing Labhead information(name, email, Affiliation)!");
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
     * Convert Contact To LabHead
     * @param labHeadContact  labHeadContact
     * @return labHead object
     */
    public LabHead convertLabHead(uk.ac.ebi.pride.data.model.Contact labHeadContact) {
        LabHead labHead = new LabHead();

        // default title for lab head is Dr
        labHead.setTitle(TitleConstants.Dr);

        //try to split the name on whitespace
        String[] tokens = labHeadContact.getName().split("\\s+");
        if (tokens.length > 1) {
            //put everything in first name except last token
            String lastName = tokens[tokens.length - 1];
            tokens[tokens.length - 1] = "";
            StringBuilder sb = new StringBuilder();
            for (String token : tokens) {
                sb.append(token).append(" ");
            }
            labHead.setFirstName(sb.toString().trim());
            labHead.setLastName(lastName);
        } else {
            labHead.setFirstName(tokens[0]);
            //set to blank string so that db doesn't barf
            labHead.setLastName(" ");
        }

        //email
        String email = labHeadContact.getEmail();
        if (email == null || "".equals(email.trim())) {
            log.warn("No email given for labHead: " + labHeadContact.toString());
        }

        labHead.setEmail(email);

        //affiliations
        String affiliation = labHeadContact.getAffiliation();
        if (affiliation == null || "".equals(affiliation)) {
            log.warn("No affiliation given for labHead: " + labHeadContact.toString());
        }
        labHead.setAffiliation(affiliation);

        return labHead;
    }

    /**
     * Convert CvParam To ProjectSampleCvParam
     * @param submissionCvParam CvParam from the Submission object
     * @return ProjectSampleCvParam object
     */
    private ProjectSampleCvParam cvParamToProjectSampleCvParam(uk.ac.ebi.pride.data.model.CvParam submissionCvParam) {

        ProjectSampleCvParam projectSampleCvParam = new ProjectSampleCvParam();
        CvParam cvParam;

        for (ProjectSampleCvParam param: modifiedProject.getSamples()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectSampleCvParam.setId(param.getId());
                break;
            }
        }

        CvParam cvParamExisting =CvParamCheckExists(submissionCvParam);
        cvParam = getCvParam(submissionCvParam, cvParamExisting);
        projectSampleCvParam.setCvParam(cvParam);
        projectSampleCvParam.setProject(modifiedProject);
        projectSampleCvParam.setValue(cvParam.getValue());
        return projectSampleCvParam;
    }

    /**
     * Use the Cv Param if exists, otherwise copy data to a new CvParam
     * @param submissionCvParam CvParam with modified values
     * @param cvParamExisting Existing CvParam from the database
     * @return CvParam
     */
    private CvParam getCvParam(uk.ac.ebi.pride.data.model.CvParam submissionCvParam, CvParam cvParamExisting) {
        CvParam cvParam;
        if(cvParamExisting != null) {
            cvParam = cvParamExisting;
        }else{
            cvParam =  new CvParam();
            cvParam.setAccession(submissionCvParam.getAccession());
            cvParam.setName(submissionCvParam.getName());
            cvParam.setCvLabel(submissionCvParam.getCvLabel());
        }
        return cvParam;
    }

    /**
     * Convert CvParam To ProjectExperimentType
     * @param submissionCvParam CvParam from the Submission object
     * @return ProjectExperimentType object
     */
    private ProjectExperimentType cvParamToProjectExperimentTypeCvParam(uk.ac.ebi.pride.data.model.CvParam submissionCvParam) {

        ProjectExperimentType projectExperimentType = new ProjectExperimentType();

        for (ProjectExperimentType param: modifiedProject.getExperimentTypes()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectExperimentType.setId(param.getId());
                break;
            }
        }

        CvParam cvParamExisting =CvParamCheckExists(submissionCvParam);
        CvParam cvParam = getCvParam(submissionCvParam, cvParamExisting);
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

        for (ProjectInstrumentCvParam param: modifiedProject.getInstruments()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectInstrumentCvParam.setId(param.getId());
                break;
            }
        }

        CvParam cvParamExisting =CvParamCheckExists(submissionCvParam);
        CvParam cvParam = getCvParam(submissionCvParam, cvParamExisting);
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

        for (ProjectQuantificationMethodCvParam param: modifiedProject.getQuantificationMethods()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectQuantificationMethodCvParam.setId(param.getId());
                break;
            }
        }

        CvParam cvParamExisting = CvParamCheckExists(submissionCvParam);
        CvParam cvParam = getCvParam(submissionCvParam, cvParamExisting);
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

        for (ProjectPTM param: modifiedProject.getPtms()) {
            if(param.getAccession().equals(submissionCvParam.getAccession())){
                projectPTM.setId(param.getId());
                break;
            }
        }

        CvParam cvParamExisting =CvParamCheckExists(submissionCvParam);
        CvParam cvParam = getCvParam(submissionCvParam, cvParamExisting);
        projectPTM.setCvParam(cvParam);
        projectPTM.setProject(modifiedProject);
        projectPTM.setValue(cvParam.getValue());
        return projectPTM;
    }

    /**
     * Check if the Cv Param is already in the database. If it is already exists, use the id of the CV param
     * to update the record. If not, leave id empty to insert a new one
     * @param submissionCvParam
     */
    private CvParam CvParamCheckExists(uk.ac.ebi.pride.data.model.CvParam submissionCvParam) {
        return cvParamList
                .stream()
                .filter(param -> param.getCvLabel().toLowerCase().trim().equals(
                        submissionCvParam.getCvLabel().toLowerCase().trim()) &&
                param.getAccession().toLowerCase().trim().equals(
                        submissionCvParam.getAccession().toLowerCase().trim()))
                .findFirst()
                .orElse(null);
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
