package uk.ac.ebi.pride.archive.pipeline.tasklets;

import lombok.extern.slf4j.Slf4j;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParamProvider;
import uk.ac.ebi.pride.archive.dataprovider.project.SubmissionType;
import uk.ac.ebi.pride.archive.pipeline.exceptions.SubmissionUpdateException;
import uk.ac.ebi.pride.archive.repo.models.param.CvParamSummary;
import uk.ac.ebi.pride.archive.repo.models.project.ProjectSummary;
import uk.ac.ebi.pride.archive.repo.models.project.ProjectTagSummary;
import uk.ac.ebi.pride.archive.repo.models.project.ReferenceSummary;
import uk.ac.ebi.pride.archive.repo.models.user.ContactSummary;
import uk.ac.ebi.pride.archive.repo.models.user.UserSummary;
import uk.ac.ebi.pride.data.model.Contact;
import uk.ac.ebi.pride.data.model.CvParam;
import uk.ac.ebi.pride.data.model.ProjectMetaData;
import uk.ac.ebi.pride.data.model.Submission;

import java.util.Collection;
import java.util.Set;

@Slf4j
public class SubmissionUpdater {

    private final CvParam NO_MODIFICATION = new CvParam("PRIDE", "PRIDE:0000398", "No PTMs are included in the dataset", null);
    private final CvParam NO_TISSUE = new CvParam("PRIDE", "PRIDE:0000442", "Tissue not applicable to dataset", null);

    /**
     * Default constructor, nothing is set.
     */
    public SubmissionUpdater() {
    }

    /**
     * Updates a Submission object with information from a ProjectSummary object.
     * @param submission the submission to be updated.
     * @param project the project with the (new) information to update.
     */
    void updateSubmission(Submission submission, ProjectSummary project) {
        ProjectMetaData projectMetaData = submission.getProjectMetaData();
        updateProjectTitle(project, projectMetaData);
        updateProjectDescription(project, projectMetaData);
        updateSampleProcessingProtocl(project, projectMetaData);
        updateDataProcessingProtocol(project, projectMetaData);
        updateKeywords(project, projectMetaData);
        updateSubmitter(project, projectMetaData);
        updateLabHead(project, projectMetaData);
        updateSubmissionType(project, projectMetaData);
        updateMassSpecExperimentMethods(project, projectMetaData);
        updateSpecies(project, projectMetaData);
        updateTissues(project, projectMetaData);
        updateCellTypes(project, projectMetaData);
        updateDisease(project, projectMetaData);
        updateInstruments(project, projectMetaData);
        updateModifications(project, projectMetaData);
        updateQuantifications(project, projectMetaData);
        updatePubmedIds(project, projectMetaData);
        updateReanalysisAccession(project, projectMetaData);
        updateProjectTags(project, projectMetaData);
    }

    /**
     * Updates the Reanalysis field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateReanalysisAccession(ProjectSummary project, ProjectMetaData projectMetaData) {
        String reanalysis = project.getReanalysis();
        projectMetaData.clearReanalysisPxAccessions();
        if (reanalysis != null) {
            String[] accessions = reanalysis.split(",");
            for (String accession : accessions) {
                projectMetaData.addReanalysisPxAccessions(accession.trim());
            }
        }
    }
    /**
     * Updates the PubMed IDs field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updatePubmedIds(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<ReferenceSummary> references = project.getReferences();
        projectMetaData.clearPubmedIds();
        if (references != null) {
            for (ReferenceSummary reference : references) {
                int pubmedId = reference.getPubmedId();
                if (pubmedId > 0) {
                    projectMetaData.addPubmedIds(pubmedId + "");
                }
            }
        }
    }

    /**
     * Updates the Quantifications field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateQuantifications(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<CvParamSummary> quantificationMethods = project.getQuantificationMethods();
        projectMetaData.clearQuantifications();
        if (quantificationMethods != null) {
            for (CvParamSummary quantificationMethod : quantificationMethods) {
                projectMetaData.addQuantifications(convertToCvParam(quantificationMethod));
            }
        }
    }

    /**
     * Updates the Modifications field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateModifications(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<CvParamSummary> ptms = project.getPtms();
        projectMetaData.clearModifications();
        if (ptms != null && !ptms.isEmpty()) {
            for (CvParamSummary ptm : ptms) {
                projectMetaData.addModifications(convertToCvParam(ptm));
            }
        } else {
            projectMetaData.addModifications(NO_MODIFICATION);
        }
    }

    /**
     * Updates the Instruments field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateInstruments(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<CvParamSummary> instruments = project.getInstruments();
        projectMetaData.clearInstruments();
        if (instruments != null) {
            for (CvParamSummary instrument : instruments) {
                projectMetaData.addInstruments(convertToCvParam(instrument));
            }
        }
    }

    /**
     * Updates the Diseasse field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateDisease(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<CvParamSummary> diseases = project.getDiseases();
        projectMetaData.clearDiseases();
        if (diseases != null) {
            for (CvParamSummary disease : diseases) {
                projectMetaData.addDiseases(convertToCvParam(disease));
            }
        }

    }
    /**
     * Updates the Cell Types field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateCellTypes(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<CvParamSummary> cellTypes = project.getCellTypes();
        projectMetaData.clearCellTypes();
        if (cellTypes != null) {
            for (CvParamSummary cellType : cellTypes) {
                projectMetaData.addCellTypes(convertToCvParam(cellType));
            }
        }
    }
    /**
     *
     * Updates the Tissues field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateTissues(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<CvParamSummary> tissues = project.getTissues();
        projectMetaData.clearTissues();
        if (tissues != null && !tissues.isEmpty()) {
            for (CvParamSummary tissue : tissues) {
                projectMetaData.addTissues(convertToCvParam(tissue));
            }
        } else {
            projectMetaData.addTissues(NO_TISSUE);
        }
    }

    /**
     * Updates the Species field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateSpecies(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<CvParamSummary> species = project.getSpecies();
        projectMetaData.clearSpecies();
        if (species != null) {
            for (CvParamSummary specy : species) {
                projectMetaData.addSpecies(convertToCvParam(specy));
            }
        }
    }

    /**
     * Updates the Mass Spec Experiment Methods field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateMassSpecExperimentMethods(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<CvParamSummary> experimentTypes = project.getExperimentTypes();
        projectMetaData.clearMassSpecExperimentMethods();
        if (experimentTypes != null) {
            for (CvParamSummary experimentType : experimentTypes) {
                projectMetaData.addMassSpecExperimentMethods(convertToCvParam(experimentType));
            }
        }
    }

    /**
     * Conerts a CV Param Provider to a proper CV Param object.
     * @param cvParamProvider the CV Param Provider to convert
     * @return a new CV Param object, based off the CV Param Provider details.
     */
    private CvParam convertToCvParam(CvParamProvider cvParamProvider) {
        return new CvParam(cvParamProvider.getCvLabel(), cvParamProvider.getAccession(), cvParamProvider.getName(), cvParamProvider.getValue());
    }

    /**
     * Updates the Submission Type field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateSubmissionType(ProjectSummary project, ProjectMetaData projectMetaData) {
        String submissionType = project.getSubmissionType();
        projectMetaData.setSubmissionType(SubmissionType.fromString(submissionType));
    }

    /**
     * Updates the Lab Head field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateLabHead(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<ContactSummary> labHeads = project.getLabHeads();
        if (labHeads != null && labHeads.size() > 0) {
            int size = labHeads.size();
            if (size > 1) {
                String msg = "Multiple lab heads found for project: " + project.getAccession() + " which has " + size + " lab heads";
                log.error(msg);
                throw new SubmissionUpdateException(msg);
            } else {
                ContactSummary labHead = labHeads.iterator().next();
                updateLabHeadContact(labHead, projectMetaData.getLabHeadContact());
            }
        } else {
            projectMetaData.setLabHeadContact(null);
        }
    }
    /**
     * Updates the Submitter field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateSubmitter(ProjectSummary project, ProjectMetaData projectMetaData) {
        UserSummary submitter = project.getSubmitter();
        Contact submitterContact = projectMetaData.getSubmitterContact();
        updateSubmitterContact(submitter, submitterContact);
    }

    /**
     * Updates a contact with information from a new contact.
     * @param newContact the contact to update with.
     * @param contactToUpdate the contact to be updated.
     */
    private void updateSubmitterContact(UserSummary newContact, Contact contactToUpdate) {
        String submitterName = newContact.getFirstName() + " " + newContact.getLastName();
        contactToUpdate.setName(submitterName);
        String email = newContact.getEmail();
        contactToUpdate.setEmail(email);
        contactToUpdate.setUserName(email);
        String affiliation = newContact.getAffiliation();
        contactToUpdate.setAffiliation(affiliation);
    }

    /**
     * Updates a contact with information from a new contact.
     * @param newContact the contact to update with.
     * @param contactToUpdate the contact to be updated.
     */
    private void updateLabHeadContact(ContactSummary newContact, Contact contactToUpdate) {
        String submitterName = newContact.getFirstName() + " " + newContact.getLastName();
        contactToUpdate.setName(submitterName);
        String email = newContact.getEmail();
        contactToUpdate.setEmail(email);
        String affiliation = newContact.getAffiliation();
        contactToUpdate.setAffiliation(affiliation);
    }

    /**
     * Updates the Keywords field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateKeywords(ProjectSummary project, ProjectMetaData projectMetaData) {
        Set<String> keywords = project.getKeywords();
        projectMetaData.setKeywords(String.join(",", keywords));
    }

    /**
     * Updates the Project Tags field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateProjectTags(ProjectSummary project, ProjectMetaData projectMetaData) {
        Collection<ProjectTagSummary> projectTags = project.getProjectTags();
        projectMetaData.clearProjectTags();
        if (projectTags != null) {
            for (ProjectTagSummary projectTag : projectTags) {
                projectMetaData.addProjectTags(projectTag.getTag());
            }
        }
    }

    /**
     * Updates the Data Processing Protocol field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateDataProcessingProtocol(ProjectSummary project, ProjectMetaData projectMetaData) {
        String dataProcessingProtocol = project.getDataProcessingProtocol();
        projectMetaData.setDataProcessingProtocol(dataProcessingProtocol);
    }

    /**
     * Updates the Sample Processing Protocol field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateSampleProcessingProtocl(ProjectSummary project, ProjectMetaData projectMetaData) {
        String sampleProcessingProtocol = project.getSampleProcessingProtocol();
        projectMetaData.setSampleProcessingProtocol(sampleProcessingProtocol);
    }

    /**
     * Updates the Project Description field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateProjectDescription(ProjectSummary project, ProjectMetaData projectMetaData) {
        String projectDescription = project.getProjectDescription();
        projectMetaData.setProjectDescription(projectDescription);
    }

    /**
     * Updates the Project Title field.
     * @param project the project information to update with.
     * @param projectMetaData the submission data to be updated.
     */
    private void updateProjectTitle(ProjectSummary project, ProjectMetaData projectMetaData) {
        String projectTitle = project.getTitle();
        projectMetaData.setProjectTitle(projectTitle);
    }
}
