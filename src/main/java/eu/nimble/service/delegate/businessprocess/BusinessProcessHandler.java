package eu.nimble.service.delegate.businessprocess;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.nimble.service.delegate.eureka.ServiceEndpoint;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class BusinessProcessHandler {

    private static Logger logger = LogManager.getLogger(BusinessProcessHandler.class);

    private static final int REQ_TIMEOUT_SEC = 3;

    private static String SERVICE_URL = "BUSINESS_PROCESS_SERVICE_BASE_URL";
    private static String SERVICE_PORT = "BUSINESS_PROCESS_SERVICE_PORT";

    public static String GET_DOCUMENT_JSON_CONTENT_PATH= "/document/json/%s";
    public static String GET_DOCUMENT_JSON_CONTENT_LOCAL_PATH= "/document/json/%s/local";
    public static String GET_DOCUMENT_XML_CONTENT_PATH= "/document/xml/%s";
    public static String GET_DOCUMENT_XML_CONTENT_LOCAL_PATH= "/document/xml/%s/local";
    public static String UPDATE_DOCUMENT_PATH= "/document/%s";
    public static String UPDATE_DOCUMENT_LOCAL_PATH= "/document/%s/local";
    public static String ARCHIVE_COLLABORATION_GROUP_PATH= "/collaboration-groups/%s/archive";
    public static String ARCHIVE_COLLABORATION_GROUP_LOCAL_PATH= "/collaboration-groups/%s/archive/local";
    public static String DELETE_COLLABORATION_GROUP_PATH= "/collaboration-groups/%s";
    public static String DELETE_COLLABORATION_GROUP_LOCAL_PATH= "/collaboration-groups/%s/local";
    public static String GET_COLLABORATION_GROUP_PATH= "/collaboration-groups/%s";
    public static String GET_COLLABORATION_GROUP_LOCAL_PATH= "/collaboration-groups/%s/local";
    public static String RESTORE_COLLABORATION_GROUP_PATH= "/collaboration-groups/%s/restore";
    public static String RESTORE_COLLABORATION_GROUP_LOCAL_PATH= "/collaboration-groups/%s/restore/local";
    public static String CANCEL_PROCESS_INSTANCE_PATH= "/processInstance/%s/cancel";
    public static String CANCEL_PROCESS_INSTANCE_LOCAL_PATH= "/processInstance/%s/cancel/local";
    public static String IS_RATED_PATH= "/processInstance/%s/isRated";
    public static String IS_RATED_LOCAL_PATH= "/processInstance/%s/isRated/local";
    public static String CREATE_RATINGS_AND_REVIEWS_PATH= "/ratingsAndReviews";
    public static String CREATE_RATINGS_AND_REVIEWS_LOCAL_PATH= "/ratingsAndReviews/local";
    public static String CONTINUE_PROCESS_INSTANCE_PATH= "/continue";
    public static String CONTINUE_PROCESS_INSTANCE_LOCAL_PATH= "/continue/local";
    public static String START_PROCESS_WITH_DOCUMENT_PATH= "/process-document";
    public static String START_PROCESS_WITH_DOCUMENT_LOCAL_PATH= "/process-document/local";
    public static String START_PROCESS_INSTANCE_PATH= "/start";
    public static String START_PROCESS_INSTANCE_LOCAL_PATH= "/start/local";
    public static String GET_ASSOCIATED_COLLABORATION_GROUP_PATH= "/processInstance/%s/collaboration-group";
    public static String GET_ASSOCIATED_COLLABORATION_GROUP_LOCAL_PATH= "/processInstance/%s/collaboration-group/local";
    public static String DELETE_PROCESS_INSTANCE_GROUP_PATH= "/process-instance-groups/%s";
    public static String DELETE_PROCESS_INSTANCE_GROUP_LOCAL_PATH= "/process-instance-groups/%s/local";
    public static String GET_PROCESS_INSTANCE_GROUP_PATH= "/process-instance-groups/%s";
    public static String GET_PROCESS_INSTANCE_GROUP_LOCAL_PATH= "/process-instance-groups/%s/local";
    public static String CANCEL_COLLABORATION_PATH= "/process-instance-groups/%s/cancel";
    public static String CANCEL_COLLABORATION_LOCAL_PATH= "/process-instance-groups/%s/cancel/local";
    public static String CHECK_COLLABORATION_FINISHED_PATH= "/process-instance-groups/%s/finished";
    public static String CHECK_COLLABORATION_FINISHED_LOCAL_PATH= "/process-instance-groups/%s/finished/local";
    public static String GET_PROCESS_INSTANCES_INCLUDED_IN_THE_GROUP_PATH= "/process-instance-groups/%s/process-instances";
    public static String GET_PROCESS_INSTANCES_INCLUDED_IN_THE_GROUP_LOCAL_PATH= "/process-instance-groups/%s/process-instances/local";
    public static String CHECK_ALL_COLLABORATIONS_FINISHED_PATH= "/collaboration-groups/all-finished";
    public static String CHECK_ALL_COLLABORATIONS_FINISHED_LOCAL_PATH= "/collaboration-groups/all-finished/local";
    public static String GET_ACTION_REQUIRED_PROCESS_COUNT_PATH= "/statistics/total-number/business-process/action-required";
    public static String GET_ACTION_REQUIRED_PROCESS_COUNT_LOCAL_PATH= "/statistics/total-number/business-process/action-required/local";
    public static String GET_PROCESS_COUNT_PATH= "/statistics/total-number/business-process";
    public static String GET_PROCESS_COUNT_LOCAL_PATH= "/statistics/total-number/business-process/local";
    public static String GET_TRADING_VOLUME_PATH= "/statistics/trading-volume";
    public static String GET_TRADING_VOLUME_LOCAL_PATH= "/statistics/trading-volume/local";
    public static String GET_AVERAGE_RESPONSE_TIME_PATH= "/statistics/response-time";
    public static String GET_AVERAGE_RESPONSE_TIME_LOCAL_PATH= "/statistics/response-time/local";
    public static String GET_AVERAGE_COLLABORATION_TIME_PATH= "/statistics/collaboration-time";
    public static String GET_AVERAGE_COLLABORATION_TIME_LOCAL_PATH= "/statistics/collaboration-time/local";
    public static String GET_AVERAGE_RESPONSE_TIME_FOR_MONTHS_PATH= "/statistics/response-time-months";
    public static String GET_AVERAGE_RESPONSE_TIME_FOR_MONTHS_LOCAL_PATH= "/statistics/response-time-months/local";
    public static String GET_RATING_SUMMARY_PATH= "/ratingsSummary";
    public static String GET_RATING_SUMMARY_LOCAL_PATH= "/ratingsSummary/local";
    public static String GET_FEDERATED_COLLABORATION_GROUP_PATH= "/collaboration-groups/federated";
    public static String GET_FEDERATED_COLLABORATION_GROUP_LOCAL_PATH= "/collaboration-groups/federated/local";
    public static String GET_COLLABORATION_GROUPS_PATH= "/collaboration-groups";
    public static String GET_COLLABORATION_GROUPS_LOCAL_PATH= "/collaboration-groups/local";
    public static String GET_PROCESS_INSTANCE_GROUP_FILTERS_PATH= "/process-instance-groups/filters";
    public static String GET_PROCESS_INSTANCE_GROUP_FILTERS_LOCAL_PATH= "/process-instance-groups/filters/local";
    public static String GET_DASHBOARD_PROCESS_INSTANCE_DETAILS_PATH= "/processInstance/%s/details";
    public static String GET_DASHBOARD_PROCESS_INSTANCE_DETAILS_LOCAL_PATH= "/processInstance/%s/details/local";
    public static String UPDATE_COLLABORATION_GROUP_NAME_PATH= "/collaboration-groups/%s";
    public static String UPDATE_COLLABORATION_GROUP_NAME_LOCAL_PATH= "/collaboration-groups/%s/local";
    public static String UPDATE_PROCESS_INSTANCE_PATH= "/processInstance";
    public static String UPDATE_PROCESS_INSTANCE_LOCAL_PATH= "/processInstance/local";
    public static String GET_ORDER_DOCUMENT_PATH= "/process-instance-groups/order-document";
    public static String GET_ORDER_DOCUMENT_LOCAL_PATH= "/process-instance-groups/order-document/local";
    public static String LIST_ALL_INDIVIDUAL_RATINGS_AND_REVIEWS_PATH= "/ratingsAndReviews";
    public static String LIST_ALL_INDIVIDUAL_RATINGS_AND_REVIEWS_LOCAL_PATH= "/ratingsAndReviews/local";
    public static String CONSTRUCT_CONTRACT_FOR_PROCESS_INSTANCE_PATH= "/contracts";
    public static String CONSTRUCT_CONTRACT_FOR_PROCESS_INSTANCE_LOCAL_PATH= "/contracts/local";
    public static String GET_CLAUSES_OF_CONTRACT_PATH= "/contracts/%s/clauses";
    public static String GET_CLAUSES_OF_CONTRACT_LOCAL_PATH= "/contracts/%s/clauses/local";
    public static String GET_CLAUSE_DETAILS_PATH= "/documents/%s/clauses";
    public static String GET_CLAUSE_DETAILS_LOCAL_PATH= "/documents/%s/clauses/local";
    public static String ADD_DOCUMENT_CLAUSE_TO_CONTRACT_PATH= "/documents/%s/contract/clause/document";
    public static String ADD_DOCUMENT_CLAUSE_TO_CONTRACT_LOCAL_PATH= "/documents/%s/contract/clause/document/local";
    public static String ADD_DATA_MONITORING_CLAUSE_TO_CONTRACT_PATH= "/documents/%s/contract/clause/data-monitoring";
    public static String ADD_DATA_MONITORING_CLAUSE_TO_CONTRACT_LOCAL_PATH= "/documents/%s/contract/clause/data-monitoring/local";
    public static String GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_PATH= "/contract/digital-agreement/%s";
    public static String GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_LOCAL_PATH= "/contract/digital-agreement/%s/local";
    public static String GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_2_PATH= "/contract/digital-agreement";
    public static String GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_2_LOCAL_PATH= "/contract/digital-agreement/local";
    public static String GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_3_PATH= "/contract/digital-agreement/all";
    public static String GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_3_LOCAL_PATH= "/contract/digital-agreement/all/local";
    public static String GET_GROUP_ID_TUPLE_PATH= "/document/%s/group-id-tuple";
    public static String GET_GROUP_ID_TUPLE_LOCAL_PATH= "/document/%s/group-id-tuple/local";
    public static String UNMERGE_COLLABORATION_GROUP_PATH= "/collaboration-groups/unmerge";
    public static String UNMERGE_COLLABORATION_GROUP_LOCAL_PATH= "/collaboration-groups/unmerge/local";
    public static String ADD_FEDERATED_METADATA_TO_COLLABORATION_GROUP_PATH= "/collaboration-groups/document/%s";
    public static String ADD_FEDERATED_METADATA_TO_COLLABORATION_GROUP_LOCAL_PATH= "/collaboration-groups/document/%s/local";
    public static String EXPORT_PROCESS_INSTANCE_DATA_PATH= "/processInstance/export";
    public static String EXPORT_PROCESS_INSTANCE_DATA_LOCAL_PATH= "/processInstance/export/local";
    public static String MERGE_COLLABORATION_GROUPS_PATH= "/collaboration-groups/merge";
    public static String MERGE_COLLABORATION_GROUPS_LOCAL_PATH= "/collaboration-groups/merge/local";
    public static String GET_STATISTICS_PATH= "/statistics/overall";
    public static String GET_STATISTICS_LOCAL_PATH= "/statistics/overall/local";
    public static String GENERATE_CONTRACT_PATH= "/contracts/create-bundle";
    public static String GENERATE_CONTRACT_LOCAL_PATH= "/contracts/create-bundle/local";
    public static String IS_PAYMENT_DONE_PATH= "/paymentDone/%s";
    public static String IS_PAYMENT_DONE_LOCAL_PATH= "/paymentDone/%s/local";
    public static String PAYMENT_DONE_PATH= "/paymentDone/%s";
    public static String PAYMENT_DONE_LOCAL_PATH= "/paymentDone/%s/local";
    public static String DELETE_DIGITAL_AGREEMENT_PATH= "/contract/digital-agreement/%s";
    public static String DELETE_DIGITAL_AGREEMENT_LOCAL_PATH= "/contract/digital-agreement/%s/local";
    public static String FINISH_COLLABORATION_PATH= "/process-instance-groups/%s/finish";
    public static String FINISH_COLLABORATION_LOCAL_PATH= "/process-instance-groups/%s/finish/local";
    public static String GET_PROCESS_INSTANCE_ID_FOR_DOCUMENT_PATH= "/processInstance/document/%s";
    public static String GET_PROCESS_INSTANCE_ID_FOR_DOCUMENT_LOCAL_PATH= "/processInstance/document/%s/local";

    public String BaseUrl;
    public int Port;
    public String PathPrefix;

    public BusinessProcessHandler() {
        try {
            BaseUrl = System.getenv(SERVICE_URL);
            try {
                Port = Integer.parseInt(System.getenv(SERVICE_PORT));
            }
            catch (Exception ex) {
                Port = -1;
            }
            String[] serviceUrlParts = BaseUrl.split("/");
            if (serviceUrlParts.length > 1) {
                BaseUrl = serviceUrlParts[0];
                PathPrefix = "/"+String.join("/", Arrays.copyOfRange(serviceUrlParts, 1, serviceUrlParts.length));
            }
            else {
                PathPrefix = "";
            }
        }
        catch (Exception ex) {
            logger.error("service env vars are not set as expected");
        }

        logger.info("Service Handler is being initialized with base url = " + BaseUrl + ", path prefix = " + PathPrefix + ", port = " + Port + "...");
    }

    public static String mergeBooleanResults(List<Future<Response>> futureList){
        // Wait (one by one) for the responses from all the services
        HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);
                if(data.contentEquals("false")){
                    return "false";
                }
            } catch(Exception e) {
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
            }
        }
        return "true";
    }

    public static String mergeDoubleResults(List<Future<Response>> futureList){
        double result = 0.0;
        // Wait (one by one) for the responses from all the services
        HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);
                result += Double.parseDouble(data);
            } catch(Exception e) {
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
            }
        }
        return Double.toString(result);
    }

    public static String mergeAverageResponseTimeForMonths(List<Future<Response>> futureList){
        Map<Integer,Double> map = new HashMap<>();

        JsonParser jsonParser = new JsonParser();

        Set<Integer> keySet = null;

        // Wait (one by one) for the responses from all the services
        HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);
                JsonObject jsonObject = jsonParser.parse(data).getAsJsonObject();
                if(keySet == null){
                    keySet = new HashSet<>();
                }
                for (String s1 : jsonObject.keySet()) {
                    Integer key = Integer.parseInt(s1);
                    Double value = jsonObject.get(s1).getAsDouble();

                    keySet.add(key);

                    if(value == 0){
                        continue;
                    }

                    if(map.containsKey(key)){
                        value = (map.get(key) + value) / 2.0;
                        map.put(key, value);
                    }
                    else{
                        map.put(key, value);
                    }
                }

            } catch(Exception e) {
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getAppName() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
            }
        }

        if(keySet != null){
            keySet.removeAll(map.keySet());
            for (Integer key : keySet) {
                map.put(key,0.0);
            }
        }

        String result = null;
        try {
            result = new ObjectMapper().writeValueAsString(map);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize map ",e);
        }
        return result;
    }

    public static String mergeRatingSummaries(List<Future<Response>> futureList){
        int responseTimeRating = 0;
        int deliveryAndPackaging = 0;
        int totalNumberOfRatings = 0;
        int qualityOfNegotiationProcess = 0;
        int qualityOfOrderingProcess = 0;
        int listingAccuracy = 0;
        int conformanceToContractualTerms = 0;
        JsonParser parser = new JsonParser();
        // Wait (one by one) for the responses from all the services
        HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);
                JsonObject object = parser.parse(data).getAsJsonObject();

                responseTimeRating += object.get("responseTimeRating").getAsInt();
                deliveryAndPackaging += object.get("deliveryAndPackaging").getAsInt();
                totalNumberOfRatings += object.get("totalNumberOfRatings").getAsInt();
                qualityOfNegotiationProcess += object.get("qualityOfNegotiationProcess").getAsInt();
                qualityOfOrderingProcess += object.get("qualityOfOrderingProcess").getAsInt();
                listingAccuracy += object.get("listingAccuracy").getAsInt();
                conformanceToContractualTerms += object.get("conformanceToContractualTerms").getAsInt();
            } catch(Exception e) {
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
            }
        }
        JsonObject object = new JsonObject();
        object.addProperty("responseTimeRating",responseTimeRating);
        object.addProperty("deliveryAndPackaging",deliveryAndPackaging);
        object.addProperty("totalNumberOfRatings",totalNumberOfRatings);
        object.addProperty("qualityOfNegotiationProcess",qualityOfNegotiationProcess);
        object.addProperty("qualityOfOrderingProcess",qualityOfOrderingProcess);
        object.addProperty("listingAccuracy",listingAccuracy);
        object.addProperty("conformanceToContractualTerms",conformanceToContractualTerms);
        return object.toString();
    }

    public static String mergeCollaborationGroups(List<ServiceEndpoint> endpointList, List<Future<Response>> futureList){
        // Wait (one by one) for the responses from all the services
        JsonArray jsonArray = new JsonArray();
        JsonParser parser = new JsonParser();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            ServiceEndpoint endpoint = endpointList.get(i);
            logger.info("got response from " + endpoint.toString());
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
                            " (" + endpoint.getHostName() +
                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);

                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("federationId",endpoint.getId());
                jsonObject.add("collaborationGroups",parser.parse(data));

                jsonArray.add(jsonObject);
            } catch(Exception e) {
                logger.warn("Exception here:{}",e.getMessage());
                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
                        " appName:" + endpoint.getAppName() +
                        " (" + endpoint.getHostName() +
                        ":" + endpoint.getPort() + ") - " +
                        e.getMessage());
            }
        }
        logger.info("aggregated results: \n" + jsonArray.toString());
        return jsonArray.toString();
    }

    public static String mergeCollaborationGroupAndFederatedCollaborations(String collaborationGroupResponsesAsString, String federatedCollaborationGroupsAsString){
        // the response
        JsonArray collaborationGroupResponse = new JsonArray();
        int size = 0;

        // federated collaboration groups
        List<Map.Entry<String,String>> pairList = new ArrayList<>();

        JsonParser parser = new JsonParser();
        JsonArray federatedCollaborationGroups = parser.parse(federatedCollaborationGroupsAsString).getAsJsonArray();
        for (JsonElement federatedCollaborationGroup : federatedCollaborationGroups) {
            String federationId = federatedCollaborationGroup.getAsJsonObject().get("federationId").getAsString();
            JsonArray object = federatedCollaborationGroup.getAsJsonObject().get("collaborationGroups").getAsJsonArray();
            if(object == null || object.size() == 0){
                continue;
            }
            for (JsonElement jsonElement : object) {
                // add collaboration group to response
                collaborationGroupResponse.add(jsonElement);
                // increment the size
                size++;
                // add collaboration groups to Map
                String collaborationId = jsonElement.getAsJsonObject().get("id").getAsString();
                pairList.add(new AbstractMap.SimpleEntry<>(federationId,collaborationId));

                JsonArray federatedCollaborationGroupMetadatas = jsonElement.getAsJsonObject().get("federatedCollaborationGroupMetadatas").getAsJsonArray();
                for (JsonElement federatedCollaborationGroupMetadata : federatedCollaborationGroupMetadatas) {
                    pairList.add(new AbstractMap.SimpleEntry<>(federatedCollaborationGroupMetadata.getAsJsonObject().get("federationID").getAsString(),federatedCollaborationGroupMetadata.getAsJsonObject().get("id").getAsString()));
                }
            }
        }

        JsonArray collaborationGroupResponses = parser.parse(collaborationGroupResponsesAsString).getAsJsonArray();
        // merge collaboration groups to federated ones
        for (JsonElement collaborationGroup : collaborationGroupResponses) {
            String federationId = collaborationGroup.getAsJsonObject().get("federationId").getAsString();
            JsonObject object = collaborationGroup.getAsJsonObject().get("collaborationGroups").getAsJsonObject();
            JsonArray collaborationGroups = object.get("collaborationGroups").getAsJsonArray();
            int groupSize = object.get("size").getAsInt();

            for (JsonElement group : collaborationGroups) {
                String id = group.getAsJsonObject().get("id").getAsString();
                // if we have this collaboration group in the response, decrement the group size and skip this group
                if(pairList.contains(new AbstractMap.SimpleEntry<>(federationId,id))){
                    groupSize--;
                    continue;
                }
                // else add this group to response
                collaborationGroupResponse.add(group);
            }
            // increment the total group size
            size += groupSize;
        }

        PriorityQueue<JsonObject> queue = new PriorityQueue<>(new CollaborationGroupComparator());
        for (JsonElement cpr : collaborationGroupResponse) {
            queue.add(cpr.getAsJsonObject());
        }
        JsonArray collaborationGroups = new JsonArray();
        for (JsonObject object : queue) {
//            System.out.println(object);
            collaborationGroups.add(object);
        }
        // TODO: limit info here
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("size",size);
        jsonObject.add("collaborationGroups",collaborationGroups);
        return jsonObject.toString();
    }

    public static String mergeProcessInstanceGroupFilters(List<Future<Response>> futureList){
        JsonArray tradingPartnerIDs = new JsonArray();
        JsonArray tradingPartnerFederationIds = new JsonArray();
        JsonArray tradingPartnerNames = new JsonArray();
        JsonArray relatedProducts = new JsonArray();
        JsonArray relatedProductCategories = new JsonArray();
        JsonArray status = new JsonArray();

        JsonParser jsonParser = new JsonParser();

        // Wait (one by one) for the responses from all the services
        HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);
                JsonObject processInstanceGroupFilter = jsonParser.parse(data).getAsJsonObject();
                JsonArray _tradingPartnerIDs = processInstanceGroupFilter.get("tradingPartnerIDs").getAsJsonArray();
                for (JsonElement tradingPartnerID : _tradingPartnerIDs) {
                    if(!tradingPartnerIDs.contains(tradingPartnerID)){
                        tradingPartnerIDs.add(tradingPartnerID);
                    }
                }
                JsonArray _tradingPartnerFederationIds = processInstanceGroupFilter.get("tradingPartnerFederationIds").getAsJsonArray();
                for (JsonElement tradingPartnerFederationId : _tradingPartnerFederationIds) {
                    if(!tradingPartnerFederationIds.contains(tradingPartnerFederationId)){
                        tradingPartnerFederationIds.add(tradingPartnerFederationId);
                    }
                }
                JsonArray _tradingPartnerNames = processInstanceGroupFilter.get("tradingPartnerNames").getAsJsonArray();
                for (JsonElement tradingPartnerName : _tradingPartnerNames) {
                    if(!tradingPartnerNames.contains(tradingPartnerName)){
                        tradingPartnerNames.add(tradingPartnerName);
                    }
                }
                JsonArray _relatedProducts = processInstanceGroupFilter.get("relatedProducts").getAsJsonArray();
                for (JsonElement relatedProduct : _relatedProducts) {
                    if(!relatedProducts.contains(relatedProduct)){
                        relatedProducts.add(relatedProduct);
                    }
                }
                JsonArray _relatedProductCategories = processInstanceGroupFilter.get("relatedProductCategories").getAsJsonArray();
                for (JsonElement relatedProductCategory : _relatedProductCategories) {
                    if(!relatedProductCategories.contains(relatedProductCategory)){
                        relatedProductCategories.add(relatedProductCategory);
                    }
                }
                JsonArray _status = processInstanceGroupFilter.get("status").getAsJsonArray();
                for (JsonElement stat : _status) {
                    if(!status.contains(stat)){
                        status.add(stat);
                    }
                }
            } catch(Exception e) {
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
            }
        }
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("tradingPartnerIDs",tradingPartnerIDs);
        jsonObject.add("tradingPartnerFederationIds",tradingPartnerFederationIds);
        jsonObject.add("tradingPartnerNames",tradingPartnerNames);
        jsonObject.add("relatedProducts",relatedProducts);
        jsonObject.add("relatedProductCategories",relatedProductCategories);
        jsonObject.add("status",status);
        return jsonObject.toString();
    }

    public static String mergeIndividualRatingsAndReviews(List<Future<Response>> futureList){
        JsonArray jsonArray = new JsonArray();

        JsonParser jsonParser = new JsonParser();

        // Wait (one by one) for the responses from all the services
        HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);
                JsonArray individualReviewsAndRatings = jsonParser.parse(data).getAsJsonArray();
                jsonArray.addAll(individualReviewsAndRatings);
            } catch(Exception e) {
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
            }
        }
        return jsonArray.toString();
    }

    public static String mergeFrameContracts(List<Future<Response>> futureList){
        JsonArray jsonArray = new JsonArray();

        JsonParser jsonParser = new JsonParser();

        // Wait (one by one) for the responses from all the services
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);
                JsonArray frameContracts = (JsonArray) jsonParser.parse(data);
                for (JsonElement frameContract : frameContracts) {
                    jsonArray.add(frameContract);
                }
            } catch(Exception e) {
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
            }
        }
        return jsonArray.toString();
    }

    public static void mergeProcessInstanceData(List<ServiceEndpoint> endpointList,List<Future<Response>> futureList, HttpServletResponse response){
        ZipOutputStream zos = null;
        try{
            logger.info("Merging process instance data");

            logger.info("adding headers to response");
            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setHeader("Access-Control-Allow-Credentials", "true");
            response.setHeader("Access-Control-Allow-Headers", "origin, content-type, accept, authorization, federationId");
            response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH");

            zos = new ZipOutputStream(response.getOutputStream());
            // Wait (one by one) for the responses from all the services
            HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
            for(int i = 0; i< futureList.size(); i++) {
                Future<Response> future = futureList.get(i);
                Response res = future.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                logger.info("Here is the future response with status:{}",res.getStatus());
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }

                InputStream data = null;
                try {
                    data = res.readEntity(InputStream.class);

                    zos.putNextEntry(new ZipEntry("transactions_" + endpointList.get(i).getId() + ".zip"));
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = data.read(buffer)) != -1) {
                        zos.write(buffer, 0, bytesRead);
                    }
                }
                catch (Exception e){
                    logger.error("Failed to create a zip:",e);
                    return;
                }
                finally {
                    if(data != null){
                        data.close();
                    }
                }

            }

            response.flushBuffer();

        }catch (Exception e){
            logger.error("Exception while merging results:",e);
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
        }finally {
            if(zos != null){
                try {
                    zos.close();
                } catch (IOException e) {
                    logger.warn("Failed to close zip output stream:",e);
                }
            }
        }

    }

    public static String mergeOverallStatistics(List<Future<Response>> futureList){
        double totalCollaborationTime = 0.0;
        double numberOfCollaborationTimeResponse = 0;
        double totalResponseTime = 0.0;
        double numberOfResponseTimeResponse = 0;
        double totalTradingVolume = 0.0;
        int totalNumberOfTransaction = 0;

        JsonParser jsonParser = new JsonParser();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            try {
                Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (res.getStatus() > 300) {
//                    logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
//                            " (" + endpoint.getHostName() +
//                            ":" + endpoint.getPort() + ")");
                    continue;
                }
                String data = res.readEntity(String.class);
                JsonObject jsonObject = jsonParser.parse(data).getAsJsonObject();

                double averageCollaborationTime = jsonObject.get("averageCollaborationTime").getAsDouble();
                double averageResponseTime = jsonObject.get("averageResponseTime").getAsDouble();
                int numberOfTransactions = jsonObject.get("numberOfTransactions").getAsInt();
                double tradingVolume = jsonObject.get("tradingVolume").getAsDouble();

                if(averageCollaborationTime != 0){
                    totalCollaborationTime += averageCollaborationTime;
                    numberOfCollaborationTimeResponse++;
                }
                if(averageResponseTime != 0){
                    totalResponseTime += averageResponseTime;
                    numberOfResponseTimeResponse++;
                }
                totalNumberOfTransaction += numberOfTransactions;
                totalTradingVolume += tradingVolume;
            } catch(Exception e) {
//                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
//                        " appName:" + endpoint.getAppName() +
//                        " (" + endpoint.getHostName() +
//                        ":" + endpoint.getPort() + ") - " +
//                        e.getMessage());
            }
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("tradingVolume",totalTradingVolume);
        jsonObject.addProperty("numberOfTransactions",totalNumberOfTransaction);
        jsonObject.addProperty("averageResponseTime",totalResponseTime == 0 ? 0: totalResponseTime/numberOfResponseTimeResponse);
        jsonObject.addProperty("averageCollaborationTime",totalCollaborationTime == 0 ? 0: totalCollaborationTime/numberOfCollaborationTimeResponse);
        return jsonObject.toString();
    }
}
