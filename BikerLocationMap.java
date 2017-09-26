package backend;

import DTOs.BikerLocationDetail;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.Tables;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.protobuf.InvalidProtocolBufferException;
import datamodel.Bikerlocation.BikerLocation;
import datamodel.BikerLocationBundle;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import play.libs.F;
import play.libs.F.*;

import utility.CommonErrors;
import utility.GeohashRange;
import utility.LocationUtils;
import utility.Utilities;

public class BikerLocationMap {
  public BikerLocationMap() {
    if (!initialized) init();
  }

  /* update flag settings */
  public static final int UPDATE_BIKER_LOCATION = 1;
  public static final int UPDATE_NEW_REQUEST = 2;
  public static final int UPDATE_ADD_REQUEST = 4;
  public static final int UPDATE_BIKER_STATUS = 3;
  public static final int UPDATE_ALL = 7;


  public Promise<BikerLocationBundle> FindAsync_biker_location(final String biker_id) {
      Promise<BikerLocationBundle> myPromise = Promise.promise(() -> Find(biker_id));
      return myPromise;
  }
  // Finds a bikerlocation in Dynamo DB.
  public BikerLocationBundle Find(String biker_id) {
    String key = Utilities.normalize(biker_id);

    // Look up in dynamo db
    GetItemResult getItemRes = null;
    try {
      Map<String, AttributeValue> itemR = new HashMap<String, AttributeValue>();
      itemR.put("BikerId", new AttributeValue().withS(key));

      GetItemRequest getItemReq = new GetItemRequest(bikerLocationTable, itemR, true);
      getItemRes = dynamoDB.getItem(getItemReq);
    } catch (AmazonServiceException ase) {
      CommonErrors.HandleServiceException(ase);
      return null;
    } catch (AmazonClientException ace) {
      CommonErrors.HandleClientException(ace);
      return null;
    }

    // Check the response.
    if (getItemRes == null || getItemRes.getItem() == null) return null;

    // Parse the data from the db into appropriate structures
    try {
      String merchantId1 = "", locationIdLevel1 = "", locationIdLevel2 = "";
      String destLocationIdLevel1 = "", destLocationIdLevel2 = "";
      String last_date_updated = ""; Boolean isActive = false;
      BikerLocation bikerLocation = null;

      for (Map.Entry<String, AttributeValue> item : getItemRes.getItem().entrySet()) {
        String attributeName = item.getKey();
        AttributeValue value = item.getValue();

        if (attributeName == "MerchantId1") {
          merchantId1 = value.getS();
        }if (attributeName == "IsActive") {
          isActive = value.getBOOL();
        } else if (attributeName == "LocationIdLevel1") {
          locationIdLevel1 = value.getS();
        } else if (attributeName == "LocationIdLevel2") {
          locationIdLevel2 = value.getS();
        } else if (attributeName == "DestLocationIdLevel1") {
          destLocationIdLevel1 = value.getS();
        } else if (attributeName == "DestLocationIdLevel2") {
          destLocationIdLevel2 = value.getS();
        } else if (attributeName == "LastDateUpdated") {
          last_date_updated = value.getN();
        } else if (attributeName == "BikerLocationDetails") {
          //byte[] b = new byte[value.getB().remaining()];
          ByteBuffer byteBuffer = value.getB();
          byteBuffer.clear();
          byte[] byteArrayTemp = new byte[byteBuffer.capacity()];  //try remaining() is not working
          byteBuffer.get(byteArrayTemp, 0, byteArrayTemp.length);   //This method retrieves all bytes in the buffer
          //.getB() gives a byte buffer which is converted to byte array
          // byte[] b = new byte[value.getB().remaining()];
          bikerLocation.parseFrom(byteArrayTemp);
        }
      }

      // Bundle to hold all meta-data related to merchant
      return new BikerLocationBundle(
          biker_id, isActive, merchantId1, locationIdLevel1, locationIdLevel2,
          destLocationIdLevel1, destLocationIdLevel2, last_date_updated,
          bikerLocation);
    } catch(Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  // updates an existing biker
  // update_flags: can be any of 1, 2, 4, 8, 16, 31
  // Depending on the values of update_flags, 1 or all of account manager,
  // billing details, merchant status, merchant profile and/or ratings would
  // get updated.
  // Fields that need to be updated should not be null values.
  // Fields that do not need to be updated could be null values.
  public boolean Update(String biker_id,
                        int update_flags,
                        String bikerStatus,
                        String merchantId1,
                        String locationIdLevel1,
                        String locationIdLevel2,
                        String destLocationIdLevel1,
                        String destLocationIdLevel2,
                        BikerLocation bikerLocation) {
    if (update_flags == 0) return false;
    String key = Utilities.normalize(biker_id);

    try {
      // Update an existing item
      Map<String, AttributeValueUpdate> updatedItem = new HashMap<String, AttributeValueUpdate>();
      Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
      item.put("BikerId", new AttributeValue().withS(key));
      //Biker Status updating
      if ((update_flags & UPDATE_BIKER_STATUS) == UPDATE_BIKER_STATUS) {
          updatedItem.put("BikerStatus", new AttributeValueUpdate()
                  .withAction(AttributeAction.PUT)
                  .withValue(new AttributeValue().withS(bikerStatus)));
      }
      if ((update_flags & UPDATE_BIKER_LOCATION) == UPDATE_BIKER_LOCATION) {
        updatedItem.put("BikerLocationDetails", new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue().withB(
                ByteBuffer.wrap(bikerLocation.toByteArray()))));
        updatedItem.put("LocationIdLevel1", new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue().withN(locationIdLevel1)));
        updatedItem.put("LocationIdLevel2", new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue().withN(locationIdLevel2)));
//          updatedItem.put("BikerStatus", new AttributeValueUpdate()
//                  .withAction(AttributeAction.PUT)
//                  .withValue(new AttributeValue().withS(bikerStatus)));

          if (biker_id.equals("1")) {
              S2CellId cellId = LocationUtils.getDestinationCell(12.979550, 77.715563);
              updatedItem.put("DestLocationIdLevel1", new AttributeValueUpdate()
                      .withAction(AttributeAction.PUT)
                      .withValue(new AttributeValue().withS(String.valueOf(cellId.id())))); //SAPlabs destlocation
          } else {
              S2CellId cellId = LocationUtils.getDestinationCell(12.996226, 77.696765);
              updatedItem.put("DestLocationIdLevel1", new AttributeValueUpdate()
                      .withAction(AttributeAction.PUT)
                      .withValue(new AttributeValue().withS(String.valueOf(cellId.id())))); //Phoenix destlocation
          }
      }
      if ((update_flags & UPDATE_NEW_REQUEST) == UPDATE_NEW_REQUEST) {
        updatedItem.put("MerchantId1", new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue().withS(merchantId1)));
        updatedItem.put("DestLocationIdLevel1", new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue().withS(destLocationIdLevel1)));
        updatedItem.put("DestLocationIdLevel2", new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue().withS(destLocationIdLevel2)));
          updatedItem.put("BikerStatus", new AttributeValueUpdate()
                  .withAction(AttributeAction.PUT)
                  .withValue(new AttributeValue().withS(bikerStatus)));
      }
      if ((update_flags & UPDATE_ADD_REQUEST) == UPDATE_ADD_REQUEST) {
        // TODO: Add MerchantId2, Dest2LocationIdLevel1, Dest2LocationIdLevel2
        return false;
      }
      updatedItem.put("LastDateUpdated", new AttributeValueUpdate()
              .withAction(AttributeAction.PUT)
              .withValue(new AttributeValue().withN(Double.toString((new Date()).getTime() / 1000))));

      ReturnValue returnValues = ReturnValue.ALL_NEW;
      UpdateItemRequest updateItemReq = new UpdateItemRequest()
              .withTableName(bikerLocationTable)
              .withKey(item)
              .withAttributeUpdates(updatedItem)
              .withReturnValues(returnValues);

      // Insert the item.
      UpdateItemResult result = dynamoDB.updateItem(updateItemReq);
      return true;
    } catch (AmazonServiceException ase) {
      CommonErrors.HandleServiceException(ase);
    } catch (AmazonClientException ace) {
      CommonErrors.HandleClientException(ace);
    }
    return false;
  }

  public boolean incrementRequestHandledCount_Conditional(Long bikerId) {
      try {
          Map<String, AttributeValueUpdate> colsToBeUpdated = new HashMap<String, AttributeValueUpdate>();
          Map<String, AttributeValue> keyCondition = new HashMap<String, AttributeValue>();
          keyCondition.put("BikerId", new AttributeValue().withS(String.valueOf(bikerId)));

          colsToBeUpdated.put("RequestCount", new AttributeValueUpdate().withAction(AttributeAction.ADD)
                  .withValue(new AttributeValue().withN("1")));

          //Use of ExpectedEntry and conditonaloperator is legacy parameter.
          //New thing is 'Condition Expression'(http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html)
          //But the legacy syntax is easy to use and sufficient for us.
          UpdateItemRequest updateItemReq = new UpdateItemRequest()
                  .withTableName(bikerLocationTable)
                  .withKey(keyCondition)
                  .withReturnValues(ReturnValue.NONE)
                  .withAttributeUpdates(colsToBeUpdated)
                  .addExpectedEntry("IsActive", new ExpectedAttributeValue()
                          .withValue(new AttributeValue().withBOOL(true))
                          .withComparisonOperator(ComparisonOperator.EQ));

          dynamoDB.updateItem(updateItemReq);
          return true;
      } catch (ConditionalCheckFailedException e) {
          //The given expected conditions have failed.Here it means the biker is not active currently.
          CommonErrors.HandleServiceException(e);
          return false;
      }
  }

  // inserts a new Merchant
  public boolean Insert(String biker_id,
                        String merchantId1,
                        String locationIdLevel1,
                        String locationIdLevel2,
                        String destLocationIdLevel1,
                        String destLocationIdLevel2,
                        BikerLocation bikerLocation) {
    String key = Utilities.normalize(biker_id);
    try {
      // Add an item
      Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
      item.put("BikerId", new AttributeValue().withS(key));
      item.put("MerchantId1", new AttributeValue().withS(merchantId1));
      item.put("LocationIdLevel1", new AttributeValue().withN(merchantId1));
      item.put("LocationIdLevel2", new AttributeValue().withN(merchantId1));
      item.put("DestLocationIdLevel1", new AttributeValue().withS(merchantId1));
      item.put("DestLocationIdLevel2", new AttributeValue().withS(merchantId1));
      item.put("bikerLocation",
               new AttributeValue().withB(ByteBuffer.wrap(bikerLocation.toByteArray())));
      item.put("LastDateUpdated",
               new AttributeValue().withN(Double.toString((new Date()).getTime() / 1000)));

      PutItemRequest putItemReq = new PutItemRequest(bikerLocationTable, item);

      // When exists is false and the id already exists a
      // ConditionalCheckFailedException will be thrown
      Map<String, ExpectedAttributeValue> expected =
        new HashMap<String, ExpectedAttributeValue>();
      expected.put("BikerId", new ExpectedAttributeValue(false));
      putItemReq.withExpected(expected);

      // Insert the item.
      PutItemResult putItemRes = dynamoDB.putItem(putItemReq);
      return true;
    } catch (ConditionalCheckFailedException e) {
      System.out.println("BikerId already exists: " + biker_id);
    } catch (AmazonServiceException ase) {
      CommonErrors.HandleServiceException(ase);
    } catch (AmazonClientException ace) {
      CommonErrors.HandleClientException(ace);
    }
    return false;
  }

    //Checks in the database for bikers in a particular level cell-id and then filters the query as per ranges we give
    //@param - locationIdLevel1 => Parent level cell-id
    //@param - ranges => The ranges with which the query has to be filtered
    //Index used - "LocationIdLevel1-BikerId-index". Hashkey used - LocationIdLevel1. Rangekey not used for querying.
    public List<BikerLocationDetail> findBikersWRTLocation(String locationIdLevel1, List<GeohashRange> ranges, S2LatLng centerLatLng) {
        List<BikerLocationDetail> bikerLocations = new ArrayList<>();
        try {
            Condition hashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ.toString())
                    .withAttributeValueList(new AttributeValue().withN(locationIdLevel1));
            Map<String, Condition> keyConditions = new HashMap<>();
            keyConditions.put("LocationIdLevel1", hashKeyCondition);

            StringBuilder filterBuilder = new StringBuilder();
            String nonKeyAttrColumnName_loc30cellId = "LocationIdLevel2";
            Map<String, AttributeValue> expressionAttrValues = new HashMap<>(); //GeohashRange range : ranges
            for (int i=1,n=ranges.size(); i<=n; i++) {
                if (i != n) {
                    filterBuilder.append("(" + nonKeyAttrColumnName_loc30cellId + " between " + ":vall" + i + " and " + ":valr" + i + ")" + " OR ");
                } else {
                    filterBuilder.append("(" + nonKeyAttrColumnName_loc30cellId + " between " + ":vall" + i + " and " + ":valr" + i + ")" );
                }
                System.out.println("Range :" + ranges.get(i-1).getRangeMinString() + "->" + ranges.get(i-1).getRangeMaxString() );
                expressionAttrValues.put(":vall" + i, new AttributeValue().withN(ranges.get(i - 1).getRangeMinString()));
                expressionAttrValues.put(":valr" + i, new AttributeValue().withN(ranges.get(i - 1).getRangeMaxString()));
//                Condition cellIdRangeCondition = new Condition().withComparisonOperator(ComparisonOperator.BETWEEN.toString())
//                        .withAttributeValueList(new AttributeValue().withS(range.getRangeMinString()), new AttributeValue().withS(range.getRangeMaxString()));
//                queryFilterConditions.put("LocationIdLevel2", cellIdRangeCondition);
            }
            String nonKeyAttrColumnName_isBikerActive = "IsActive";
            filterBuilder.append(" AND (" + nonKeyAttrColumnName_isBikerActive + " = :isActiveVal" + ")");
            expressionAttrValues.put(":isActiveVal", new AttributeValue().withBOOL(true));

            System.out.println("Sending AWS request for hashkey: " + locationIdLevel1 + "; ranges: " + ranges.size());
            QueryRequest queryRequest = new QueryRequest().withTableName(bikerLocationTable).withKeyConditions(keyConditions)
                    .withSelect(Select.COUNT)
                    .withSelect(Select.ALL_ATTRIBUTES)
                    .withFilterExpression(filterBuilder.toString())
                    .withExpressionAttributeValues(expressionAttrValues)
                    .withScanIndexForward(true)
                    .withConsistentRead(false);   //scanindexforward - true gives 1 to 10 order
//            //ConditionalOperator can only be used when Filter or Expected has two or more elements
//            if (queryFilterConditions.size() >= 2){
//                queryRequest.withConditionalOperator(ConditionalOperator.OR);
//            }

            queryRequest.setIndexName("LocationIdLevel1-BikerId-index");
            QueryResult queryResult = dynamoDB.query(queryRequest);

            if (queryResult.getCount() == 0) {
                System.out.println("Count returned 0 for parent:" + locationIdLevel1);
                return bikerLocations;
            }
            for (Map<String, AttributeValue> item : queryResult.getItems()) {
                BikerLocationDetail temp = new BikerLocationDetail();
                for (Map.Entry<String, AttributeValue> attribute : item.entrySet()) {
                    switch (attribute.getKey()) {
                        case "BikerId":
                            temp.bikerId = attribute.getValue().getS();
                            break;
                        case "DestLocationIdLevel1":
                            temp.destLocationIdLevel1 = Long.valueOf(attribute.getValue().getS());
                            break;
                        case "DestLocationIdLevel2":
                            temp.destLocationIdLevel2 = Long.valueOf(attribute.getValue().getS());
                            break;
                        case "LastDateUpdated":
                            temp.lastDateUpdated = Long.valueOf(attribute.getValue().getN());
                            break;
                        case "LocationIdLevel1":
                            temp.locationIdLevel1 = Long.valueOf(attribute.getValue().getN());
                            break;
                        case "LocationIdLevel2":
                            temp.locationIdLevel2 = Long.valueOf(attribute.getValue().getN());
                            break;
                        case "MerchantId1":
                            temp.merchantId = attribute.getValue().getS();
                            break;
                        case "TimeId1":
                            temp.timeId1 = attribute.getValue().getS();
                            break;
                        case "BikerLocationDetails":
                            ByteBuffer byteBuffer = attribute.getValue().getB();
                            byteBuffer.clear();
                            byte[] byteArrayTemp = new byte[byteBuffer.capacity()];  //try remaining() is not working
                            byteBuffer.get(byteArrayTemp, 0, byteArrayTemp.length);   //This method retrieves all bytes in the buffer
                            //.getB() gives a byte buffer which is converted to byte array
                            temp.bikerLocation = BikerLocation.parseFrom(byteArrayTemp);  //The byte buffer is now converted to proto buffer object
                    }
                }
                temp.merchantLatLng = centerLatLng;
                bikerLocations.add(temp);
            }//End of second for
        } catch (AmazonServiceException ase) {
            CommonErrors.HandleServiceException(ase);
            return bikerLocations; //Would be an empty list
        } catch (AmazonClientException ace) {
            CommonErrors.HandleClientException(ace);
            return bikerLocations;  //Would be an empty list
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return bikerLocations;
        }

        return bikerLocations.stream().sorted(BikerLocationDetail.comparator).collect(Collectors.toList());
    }//End of method

  // Initializes the client for all dynamodb operations
  private static void init() {
    try {
      /*
       * This credentials provider implementation loads your AWS credentials
       * from a properties file at the root of your classpath.
       */
      dynamoDB = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
      //For dynamodb local use . Remove whilecommiting
      //  dynamoDB.setEndpoint("http://localhost:8000");
        //For dynamodb local use . Add whilecommiting
      Region apSouthEast1 = Region.getRegion(Regions.AP_SOUTHEAST_1);
      dynamoDB.setRegion(apSouthEast1);
    } catch(AmazonServiceException ase) {
      CommonErrors.HandleServiceException(ase);
    } catch (AmazonClientException ace) {
      CommonErrors.HandleClientException(ace);
    }
  }

  /* AWS dynamo db credentials */
  static AmazonDynamoDBClient dynamoDB;
  static boolean initialized = false;

  /* AWS dynamo db table name. */
  private final String bikerLocationTable = "BikerLocation";

  public static void main(String[] args) {
    System.out.println("Testing insert merchant profile."); // Display the string.

    BikerLocationMap map = new BikerLocationMap();
    for (int i = 1; i < 5; i++) {
      BikerLocation location = BikerLocation.newBuilder().setBikeLicensePlate("TestStore").build();

      map.Insert(Integer.toString(i), "merchant1", "location1", "location2", "dest1", "dest2", location);
      map.Update(Integer.toString(i), UPDATE_BIKER_LOCATION,"1", "", "location1", "location3", "", "", location);
    }
    System.out.println("Merchant Id: " + (map.Find("1")).merchant_id1());
  }
}

