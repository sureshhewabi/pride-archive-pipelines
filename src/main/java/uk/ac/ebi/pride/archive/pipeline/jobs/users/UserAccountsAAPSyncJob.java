package uk.ac.ebi.pride.archive.pipeline.jobs.users;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.dataprovider.utils.RoleConstants;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;

import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.repos.user.User;
import uk.ac.ebi.pride.archive.repo.repos.user.UserAAP;
import uk.ac.ebi.pride.archive.repo.repos.user.UserRepository;
import uk.ac.ebi.pride.archive.repo.util.AAPConstants;
import uk.ac.ebi.pride.archive.repo.util.ObjectMapper;

import java.nio.charset.Charset;
import java.util.*;

@Configuration
@Slf4j
@Import({ArchiveOracleConfig.class, DataSourceConfiguration.class})
public class UserAccountsAAPSyncJob extends AbstractArchiveJob {

    @Value("${aap.auth.url}")
    private String aapAuthURL;

    @Value("${aap.auth.hash.url}")
    private String aapAuthHashURL;

    @Value("${aap.domain.management.url}")
    private String aapDomainMngmtURL;

    @Value("${aap.pride.service.uname}")
    private String aapUname;

    @Value("${aap.pride.service.pwd}")
    private String aapPwd;

    @Value("${aap.domain.url}")
    private String aapDomainsURL;

    @Value("${aap.user.search.url}")
    private String aapUserSearchURL;

    @Autowired
    UserRepository userRepository;

    private RestTemplate restTemplate;
    private String aapToken;
    private Map<String,String> prideAAPDomainsMap;
    private volatile int counter=0;

    private void initializeParameters() {
        restTemplate = new RestTemplate();
        getAAPToken();
        getAAPDomains();
    }

    /*Used to send AAP token in the headers of requests*/
    private HttpHeaders createAAPTokenAuthHeaders(){
        HttpHeaders headers = new HttpHeaders();
        headers.add( "Authorization", "Bearer "+aapToken );
        /*headers.setContentType(MediaType.APPLICATION_PROBLEM_JSON_UTF8);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON_UTF8));*/
        return headers;
    }

    /*To get AAP token initially*/
    private HttpHeaders createBasicAuthHeaders(String username, String password){
        HttpHeaders headers = new HttpHeaders();
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(
                auth.getBytes(Charset.forName("UTF-8")) );
        String authHeader = "Basic " + new String( encodedAuth );

        headers.add( "Authorization", authHeader );
        headers.setContentType(MediaType.APPLICATION_PROBLEM_JSON_UTF8);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON_UTF8));
        return headers;
    }

    private void getAAPToken(){
        ResponseEntity<String> responseEntity = restTemplate.exchange(aapAuthURL+"?ttl=180", HttpMethod.GET,new HttpEntity(createBasicAuthHeaders(aapUname,aapPwd)),String.class);
        log.info("getAAPToken() Status:"+responseEntity.getStatusCode());
        if(responseEntity.getStatusCode().is2xxSuccessful()){
            aapToken=responseEntity.getBody();
        }
    }

    private void getAAPDomains(){
        ResponseEntity<String> responseEntity = restTemplate.exchange(aapDomainMngmtURL, HttpMethod.GET,new HttpEntity(createAAPTokenAuthHeaders()),String.class);
        if(!responseEntity.getStatusCode().is2xxSuccessful()){
            log.error("Cannot retrieve PRIDE domains. Error code:"+responseEntity.getStatusCode()+"  and response body:"+responseEntity.getBody());
            return;
        }
        JSONArray domainsJsonArray = new JSONArray(responseEntity.getBody());
        prideAAPDomainsMap = new HashMap<String,String>();
        for(int i=0;i<domainsJsonArray.length();i++){
            JSONObject domainJsonObj = domainsJsonArray.getJSONObject(i);
            prideAAPDomainsMap.put(domainJsonObj.getString(AAPConstants.DOMAIN_NAME),domainJsonObj.getString(AAPConstants.DOMAIN_REF));
        }

    }

    //check if user exists in AAP with same pride email<==>AAP username and filter the list
    public Tuple<Boolean,String> checkUserInAAP(String email, Long id){
        ResponseEntity<String> responseEntity;
        try{
            responseEntity = restTemplate.exchange(aapUserSearchURL+"?value="+email, HttpMethod.GET,new HttpEntity(createAAPTokenAuthHeaders()),String.class);
            log.info("checkUserInAAP status for id:"+id+" => "+responseEntity.getStatusCode());
            String responseBody = responseEntity.getBody();
            if(responseBody!=null && responseBody.contains(AAPConstants.USER_REF)){
                JSONArray userSearchArray = new JSONArray(responseBody);
                JSONObject userSearchJsonObj = (JSONObject) userSearchArray.get(0);
                return new Tuple(true,(String)(userSearchJsonObj.get(AAPConstants.USER_REF)));
            }else{
                log.warn("userReference not available in JSON response: "+responseBody+" and code:"+responseEntity.getStatusCode());
            }
        }catch(HttpClientErrorException e){
            log.warn("userReference not available in JSON response: "+e.getResponseBodyAsString()+" and code:"+e.getStatusCode());
        }


        return new Tuple(false,null);
    }

    public void createUserInAAP(User user) {
        UserAAP userAAP = ObjectMapper.mapUsertoUserAAP(user);

        //create json for syncing account on hashed endpoint
        JSONObject requestJson = new JSONObject();
        requestJson.put(AAPConstants.USER_UNAME,userAAP.getUsername());
        requestJson.put(AAPConstants.USER_HASHED_PWD,userAAP.getPassword());
        requestJson.put(AAPConstants.USER_EMAIL,userAAP.getEmail());
        requestJson.put(AAPConstants.USER_FULL_NAME,userAAP.getName());
        requestJson.put(AAPConstants.USER_ORG,userAAP.getOrganization());
        //setting up the request headers
        HttpHeaders requestHeaders = createBasicAuthHeaders(aapUname,aapPwd);//new HttpHeaders();
        requestHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);
        requestHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_JSON_UTF8));

        //request entity is created with request body and headers
        HttpEntity<String> requestEntity = new HttpEntity<>(requestJson.toString(), requestHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(
                aapAuthHashURL,
                HttpMethod.POST,
                requestEntity,
                String.class);

        if(responseEntity.getStatusCode().is2xxSuccessful()) {
            user.setUserRef(responseEntity.getBody());
            userRepository.save(user);
            log.info("User:"+user.getId()+" saved successfully in AAP and updated in PRIDE");
        }else {
            log.error("Creating user in AAP returned error code:"+responseEntity.getStatusCode()+" and body:"+responseEntity.getBody());
        }

    }

    public void updateAAPDomainMembership(User user){
        //get user's authorities from PRIDE
        for(RoleConstants userAuthority : user.getUserAuthorities()){
            switch (userAuthority){
                case ADMINISTRATOR:
                    addUserToAAPDomain(user.getUserRef(),prideAAPDomainsMap.get(AAPConstants.PRIDE_ADMINISTRATOR_DOMAIN));
                    break;
                case SUBMITTER:
                    addUserToAAPDomain(user.getUserRef(),prideAAPDomainsMap.get(AAPConstants.PRIDE_SUBMITTER_DOMAIN));
                    break;
                case REVIEWER:
                    addUserToAAPDomain(user.getUserRef(),prideAAPDomainsMap.get(AAPConstants.PRIDE_REVIEWER_DOMAIN));
                    break;
            }
        }
        addUserToAAPDomain(user.getUserRef(),prideAAPDomainsMap.get(AAPConstants.PRIDE_DOMAIN));

        log.info("Processing record:"+(++counter));

    }

    private void addUserToAAPDomain(String userRef, String domainRef) {
        ResponseEntity<String> responseEntity = restTemplate.exchange(aapDomainsURL+"/"+domainRef+"/"+userRef+"/user", HttpMethod.PUT,new HttpEntity(createAAPTokenAuthHeaders()),String.class);
        if(!responseEntity.getStatusCode().is2xxSuccessful()){
            log.error("User:"+userRef+" not added to domain:"+domainRef+" Error code:"+responseEntity.getStatusCode()+" and error body:"+responseEntity.getBody());
        }
    }


    //get users from PRIDE with USER_AAP_REF as null
    @Bean
    Step syncPrideUsersToAAP() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_USERS_AAP_SYNC.name())
                .tasklet((stepContribution, chunkContext) -> {

                    initializeParameters();

                    /*List<User> localUsersList = userRepository.findFilteredLocalUsers();
                    System.out.println("Local filtered users size =>"+localUsersList.size());
                    if(localUsersList.size()>0){
                        int index=0,size=localUsersList.size();
                        for(User localUser : localUsersList){
                            log.info("Processing record:"+(++index)+"/"+size);
                            if(localUser.getEmail() == null || localUser.getEmail().trim().length()==0){
                                log.error("User email null for id:"+localUser.getId());
                                continue;
                            }
                            Tuple<Boolean,String> resultTuple = checkUserInAAP(localUser.getEmail(),localUser.getId());
                            if(resultTuple.getKey()){
                                *//*if user already exists
                                  save user ref in DB
                                 *//*
                                localUser.setUserRef(resultTuple.getValue());
                                userRepository.save(localUser);
                            }else {
                                //create a new user in AAP
                                createUserInAAP(localUser);
                            }
                            //update user domain memberships in AAP
                            updateAAPDomainMembership(localUser);
                        }
                    }*/

                    userRepository.findFilteredLocalUsers().parallelStream().forEach(localUser -> {
                        if(localUser.getEmail() == null || localUser.getEmail().trim().length()==0){
                            log.error("User email null for id:"+localUser.getId());
                            return;
                        }
                        Tuple<Boolean,String> resultTuple = checkUserInAAP(localUser.getEmail(),localUser.getId());
                        if(resultTuple.getKey()){
                                /*if user already exists
                                  save user ref in DB
                                 */
                            localUser.setUserRef(resultTuple.getValue());
                            userRepository.save(localUser);
                        }else {
                            //create a new user in AAP
                            createUserInAAP(localUser);
                        }
                        //update user domain memberships in AAP
                        updateAAPDomainMembership(localUser);
                    });

                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    //sync remaining elements into AAP
    @Bean
    public Job importUserJob() {
        return jobBuilderFactory.get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_USERS_AAP_SYNC.getName())
                .incrementer(new RunIdIncrementer())
                .flow(syncPrideUsersToAAP())
                .end()
                .build();
    }

}