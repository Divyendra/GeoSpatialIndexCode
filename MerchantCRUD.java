package controllers;

/**
 * Created by Karthik on 3/9/15.
 */
import java.util.*;
import javax.inject.*;

import DTOs.BikerLocationDetail;
import backend.BikerLocationMap;
import com.amazonaws.services.opsworks.model.AttachElasticLoadBalancerRequest;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import datamodel.Bikerlocation;
import datamodel.Merchant;
import play.api.libs.concurrent.Promise;
import play.libs.ws.*;

import play.*;
import play.mvc.*;
import play.cache.Cache;
import play.data.Form;
import play.libs.F;
import play.libs.F.Function0;
import play.Logger;
import play.mvc.Http.*;
import play.data.Form;

import backend.MerchantMap;
import datamodel.Merchant.MerchantProfile;
import datamodel.Merchant.MerchantData;
import datamodel.Merchant.MerchantBillingDetails;
import datamodel.Merchant.MerchantStatus;
import datamodel.MerchantProfileBundle;
import utility.Constants;
import utility.GeohashRange;
import utility.LocationUtils;
import utility.Utilities;

public class MerchantCRUD extends Controller {

    private static final String REDIRECT_ERROR_MESSAGE = "Error. Please retry.";
    private static final String UNKNOWN_USER = "UNKNOWN";
    public static Logger.ALogger merchantCRUDLogger = Logger.of("application.controllers.merchantCRUD");
    @Inject Utilities utilities;

    //Merchant login
    public Result login() {
        String merchantId = "";
        String password = "";
        try {
            //Execution start time
            long exeStartTime = System.currentTimeMillis();
            byte[] byteArray = request().body().asRaw().asBytes();
            MerchantProfile merchantProfile = MerchantProfile.parseFrom(byteArray);

            merchantId = merchantProfile.getPhone();
            password = merchantProfile.getPassword();

            if(merchantCRUDLogger.isInfoEnabled()) {
                merchantCRUDLogger.info("Request for login from merchant " + merchantId);
            }
            MerchantMap map = new MerchantMap();
            if(merchantCRUDLogger.isDebugEnabled()) {
                merchantCRUDLogger.debug("Attempting find of userProfile");
            }
            MerchantProfileBundle merchantProfileBundle = map.Find(merchantId);
            if (merchantProfileBundle != null) {
                if (merchantCRUDLogger.isInfoEnabled()) {
                    merchantCRUDLogger.info("Merchant profile exists for this phone number.");
                }
                MerchantProfile merchantResponseProfile = merchantProfileBundle.profile();
                if (password.equals(merchantResponseProfile.getPassword())) {
                    return ok(merchantProfile.toByteArray());
                } else {
                    if(merchantCRUDLogger.isErrorEnabled()) {
                        merchantCRUDLogger.error("Merchant profile password doesn't match with entered password");
                    }
                    return ok(Constants.ResponseStatusCode.PASSWORD_MISMATCH);
                }
            } else {
                if(merchantCRUDLogger.isErrorEnabled()) {
                    merchantCRUDLogger.error("Merchant profile doesn't exists");
                }
                return ok(Constants.ResponseStatusCode.PROFILE_NOT_EXISTS);
            }
        } catch(Exception e) {
            if(merchantCRUDLogger.isErrorEnabled()) {
                merchantCRUDLogger.error("Some exception from merchant login " + merchantId, e);
            }
            return ok(Constants.ResponseStatusCode.FAILED);
        }
    }

    //Merchant new registration
    public Result register() {
        String merchantId = "";
        try {
            //Execution start time
            long exeStartTime = System.currentTimeMillis();
            byte[] byteArray = request().body().asRaw().asBytes();
            MerchantData merchantData = MerchantData.parseFrom(byteArray);
            merchantId = merchantData.getMerchantProfile().getPhone();
            if(merchantCRUDLogger.isInfoEnabled()) {
                merchantCRUDLogger.info("Request for New Registration from user " + merchantId);
            }
            MerchantMap map = new MerchantMap();
            int retval = map.Insert(merchantId, "123456", merchantData.getMerchantBillingDetails(), merchantData.getMerchantStatus(), merchantData.getMerchantProfile(), "2.5");
            switch (retval) {
                case Constants.DynamoStatusCode.SUCCESS:
                    if(merchantCRUDLogger.isInfoEnabled()) {
                        merchantCRUDLogger.info("Insert Success Execution time is" + (System.currentTimeMillis() - exeStartTime));
                    }
                    return ok(Constants.ResponseStatusCode.SUCCESS);
                case Constants.DynamoStatusCode.DUPLICATE_ITEM:
                    if(merchantCRUDLogger.isInfoEnabled()) {
                        merchantCRUDLogger.info("Profile already exist Execution time is" + (System.currentTimeMillis() - exeStartTime));
                    }
                    return ok(Constants.ResponseStatusCode.DUPLICATE);
                case Constants.DynamoStatusCode.DYNAMO_UNREACHABLE:
                case Constants.DynamoStatusCode.OTHER_ERRORS:
                    if(merchantCRUDLogger.isInfoEnabled()) {
                        merchantCRUDLogger.info("Some server exception");
                    }
                    return ok(Constants.ResponseStatusCode.FAILED);
            }
        } catch(Exception e) {
            if(merchantCRUDLogger.isErrorEnabled()) {
                merchantCRUDLogger.error("Some exception from biker profile creation " + e);
            }
        }
        return ok(Constants.ResponseStatusCode.FAILED);
    }

    public F.Promise<Result> findBikersInProximity() {
        long start = System.currentTimeMillis();
        double radiusInMeterNw = 600;
        double merchantLat = 12.977821;
        double merchantLong = 77.709898;
        S2LatLng centerLatLng = S2LatLng.fromDegrees(merchantLat, merchantLong);
        HashMap<Long, List<GeohashRange>> rangesSearchable = LocationUtils.getCellIdRangesSurrounding(merchantLat, merchantLong, centerLatLng, radiusInMeterNw);
        Iterator it = rangesSearchable.entrySet().iterator();
        BikerLocationMap bikerLocationMap = new BikerLocationMap();
        List<F.Promise<List<BikerLocationDetail>>> promiseList = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            Long parentLevel = (Long) pair.getKey();
            System.out.println("***********Parent Level - " + parentLevel+ "***********");
            List<GeohashRange> rangeList = (ArrayList < GeohashRange >)pair.getValue();
            System.out.println("Ranges" + "(" + rangeList.size() + ")" + " : ");
            F.Promise<List<BikerLocationDetail>> promise = F.Promise.promise(() -> bikerLocationMap.findBikersWRTLocation(String.valueOf(parentLevel), rangeList, centerLatLng));
            promiseList.add(promise);
            //The below is just for testing the genuinity of the LocationUtils Algo.
//            for (GeohashRange iRange: rangeList ) {
//                System.out.println(iRange.getRangeMin() + " -> " + iRange.getRangeMax());
//                if (!(new S2CellId(iRange.getRangeMin()).parent(LocationUtils.HASHKEY_LEVEL).id() == new S2CellId(iRange.getRangeMax()).parent(LocationUtils.HASHKEY_LEVEL).id())) {
//                    System.out.println("Something went wrong mate: " + new S2CellId(iRange.getRangeMin()).parent(LocationUtils.HASHKEY_LEVEL).id() + ";" + new S2CellId(iRange.getRangeMax()).parent(LocationUtils.HASHKEY_LEVEL).id());
//                }
//            }
        }

        //Below is the actual code to be used when proto comes to picture
//        F.Promise<Result> resultPromise = F.Promise.sequence(promiseList).map(lists -> {
//            Bikerlocation.BikersNearBy bikersNearBy = null;
//            if (lists != null) {
//                if (!lists.isEmpty()) {
//                    //The individual lists here are sorted already. So for sorting 'list of sortedlists', use Utilities.mergeSortedLists()
//                    List<BikerLocationDetail> combinedSortedList = lists.get(0);
//                    for (int i = 1, n = lists.size(); i < n; i++) {
//                        combinedSortedList = Utilities.mergeSortedLists(combinedSortedList, lists.get(i));
//                    }
//                    //ProtoList
//                    List<Bikerlocation.BikerLocation> bikerLocationList = Utilities.POJOToProto.convert(combinedSortedList);
//                    bikersNearBy = Bikerlocation.BikersNearBy.newBuilder().addAllBikerLocations(bikerLocationList).build();
//                    return ok(bikersNearBy.toByteArray());
//                } else {
//                    //notFound when no biker are found around a merchant's location.
//                    return notFound(bikersNearBy.toByteArray());
//                }
//            } else {
//                //internalServerError for when bikerlist is not intiated in mapperclass. Shouldn't happen normally.
//                return internalServerError(bikersNearBy.toByteArray());
//            }
//        });

        F.Promise<Result> resultPromise = F.Promise.sequence(promiseList).map(lists -> {
            System.out.println("In Promise.sequence()..");
            if (lists != null) {
                if (!lists.isEmpty()) {
                    List<BikerLocationDetail> combinedSortedList = lists.get(0);
                    for (int i = 1, n = lists.size(); i < n; i++) {
                        //The individual lists here are sorted already. So for sorting 'list of sortedlists', use Utilities.mergeSortedLists()
                        combinedSortedList = Utilities.mergeSortedLists(combinedSortedList, lists.get(i));
                    }

                    if (!combinedSortedList.isEmpty()) {
                        StringBuilder stringBuilder = new StringBuilder();
                        for (BikerLocationDetail iDetail : combinedSortedList) {
                            stringBuilder.append("id- " + iDetail.bikerId + " distance- " + iDetail.distanceFromMerchant + "\n");
                        }
                        System.out.println("Executed in :" + (System.currentTimeMillis() - start) + "ms");
                        return ok(stringBuilder.toString());
                    } else {
                        System.out.println("Executed in :" + (System.currentTimeMillis() - start) + "ms");
                        return ok("Not Found any bikers nearby");
                    }
                } else {
                    //notFound when no biker are found around a merchant's location.
                    System.out.println("Executed in :" + (System.currentTimeMillis() - start) + "ms");
                    return ok("Not Found a any bikers nearby");
                }
            } else {
                //internalServerError for when bikerlist is not intiated in mapperclass. Shouldn't happen normally.
                System.out.println("Executed in :" + (System.currentTimeMillis() - start) + "ms");
                return internalServerError(Constants.SERVER_EXCEPTION);
            }
        });
        return resultPromise;
    }
}
