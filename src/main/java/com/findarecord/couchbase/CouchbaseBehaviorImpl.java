package com.findarecord.couchbase;



import com.couchbase.capi.CouchbaseBehavior;
import com.findarecord.App;

import java.util.*;

public class CouchbaseBehaviorImpl implements CouchbaseBehavior {

  protected String hostname;
  protected Integer port;

  public CouchbaseBehaviorImpl(String hostname, Integer port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public List<String> getPools() {
        /*
        As I understand it, always return the string "default"
         */
    //System.out.println("getPools");
    List<String> result = new ArrayList<>();
    result.add("default");
    return result;
  }

  @Override
  public String getPoolUUID(String pool) {
        /*
        Always return the uuid using the pool name.
        If we ever want to support multiple neo4j clusters, we need to base this off of the unique cluster name
         */
    //System.out.println("getPoolUUID");
    //System.out.println("in: "+pool);
    //System.out.println("out: "+ret);
    return UUID.nameUUIDFromBytes(hostname.getBytes()).toString().replace("-", "");

  }

  @Override
  public Map<String, Object> getPoolDetails(String pool) {
        /*
        If pool is default, return buckets and nodes. else return null
         */
    if("default".equals(pool)) {
      Map<String, Object> bucket = new HashMap<>();
      bucket.put("uri", "/pools/" + pool + "/buckets?uuid=" + getPoolUUID(pool));

      Map<String, Object> responseMap = new HashMap<>();
      responseMap.put("buckets", bucket);

      List<Object> nodes = getNodesServingPool(pool);
      responseMap.put("nodes", nodes);

      //System.out.println("getPoolDetails");
      //System.out.println("in: "+pool);
      //System.out.println("out: "+responseMap);

      return responseMap;
    }
    return null;
  }

  @Override
  public List<String> getBucketsInPool(String pool) {
        /*
        We will always return the same buckets, enabling a map to collections, places, etc.
         */
    if("default".equals(pool)) {
      List<String> bucketNameList = new ArrayList<>();

      bucketNameList.add("collections");
      bucketNameList.add("places");
      bucketNameList.add("repos");
      bucketNameList.add("entries");

      //System.out.println("getBucketsInPool");
      //System.out.println("in: "+pool);
      //System.out.println("out: "+bucketNameList);

      return bucketNameList;
    }
    return null;
  }

  @Override
  public String getBucketUUID(String pool, String bucket) {
        /*
        Only return if it is a bucket we actually have
         */
    if("default".equals(pool))  {
      if("collections".equals(bucket) || "places".equals(bucket) || "repos".equals(bucket)|| "entries".equals(bucket)) {
        //System.out.println("getBucketUUID");
        //System.out.println("in: "+pool);
        //System.out.println("in: "+bucket);
        //System.out.println("out: "+ret);
        return UUID.nameUUIDFromBytes(bucket.getBytes()).toString().replace("-", "");
      }
      return null;
    }
    return null;
  }

  @Override
  public List<Object> getNodesServingPool(String pool) {
        /*
        There is only ever one node, the one that this plugin runs on.
         */
    if("default".equals(pool)) {
      List<Object> nodes = new ArrayList<>();

      Map<String, Object> nodePorts = new HashMap<>();
      nodePorts.put("direct", port);

      Map<String, Object> node = new HashMap<>();
      String hostPort = hostname + ":" + port.toString();
      node.put("couchApiBase", String.format("http://%s/", hostPort));
      node.put("hostname", hostPort);
      node.put("ports", nodePorts);
      nodes.add(node);

      //System.out.println("getNodesServingPool");
      //System.out.println("in: "+pool);
      //System.out.println("out: "+nodes);

      return nodes;
    }
    return null;
  }

  @Override
  public Map<String, Object> getStats() {
    //System.out.println("getStats");
    return new HashMap<>();
  }


}
