package com.findarecord.couchbase;

import com.couchbase.capi.CAPIBehavior;

import org.codehaus.jackson.map.ObjectMapper;
import org.apache.log4j.Logger;

import javax.servlet.UnavailableException;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

public class CouchbaseCAPIBehaviorImpl implements CAPIBehavior {

  protected ObjectMapper mapper = new ObjectMapper();
  protected Logger logger;
  protected Semaphore activeRequests;


  public CouchbaseCAPIBehaviorImpl(int maxConcurrentRequests, Logger logger) {
    this.activeRequests = new Semaphore(maxConcurrentRequests);
    this.logger = logger;
  }

  @Override
  public boolean databaseExists(String database) {
    String db = getElasticSearchIndexNameFromDatabase(database);
    return "collections".equals(db) || "places".equals(db) || "repos".equals(db) || "entries".equals(db);

  }

  @Override
  public Map<String, Object> getDatabaseDetails(String database) {
    if(databaseExists(database)) {
      Map<String, Object> responseMap = new HashMap<>();
      responseMap.put("db_name", getDatabaseNameWithoutUUID(database));
      return responseMap;
    }
    return null;
  }

  @Override
  public boolean createDatabase(String database) {
    throw new UnsupportedOperationException("Attachments are not supported");
  }

  @Override
  public boolean deleteDatabase(String database) {
    throw new UnsupportedOperationException("Attachments are not supported");
  }

  @Override
  public boolean ensureFullCommit(String database) {
    return true;
  }

  @Override
  public Map<String, Object> revsDiff(String database, Map<String, Object> revsMap) throws UnavailableException {

    // start with all entries in the response map
    Map<String, Object> responseMap = new HashMap<>();
    for (Entry<String, Object> entry : revsMap.entrySet()) {
      String id = entry.getKey();
      String revs = (String)entry.getValue();
      Map<String, String> rev = new HashMap<>();
      rev.put("missing", revs);
      responseMap.put(id, rev);
    }

    return responseMap;
  }

  @Override
  public List<Object> bulkDocs(String database, List<Map<String, Object>> docs) throws UnavailableException {
    List<Object> result = new ArrayList<>();

    return result;
  }

  @Override
  public Map<String, Object> getDocument(String database, String docId) {
    return getLocalDocument(database, docId);
  }

  @Override
  public Map<String, Object> getLocalDocument(String database, String docId) {
    return null;
  }

  @Override
  public String storeDocument(String database, String docId, Map<String, Object> document) {
    return storeLocalDocument(database, docId, document);
  }

  @Override
  public String storeLocalDocument(String database, String docId, Map<String, Object> document) {
    return null;
  }

  @Override
  public InputStream getAttachment(String database, String docId, String attachmentName) {
    throw new UnsupportedOperationException("Attachments are not supported");
  }

  @Override
  public String storeAttachment(String database, String docId, String attachmentName, String contentType, InputStream input) {
    throw new UnsupportedOperationException("Attachments are not supported");
  }

  @Override
  public InputStream getLocalAttachment(String databsae, String docId, String attachmentName) {
    throw new UnsupportedOperationException("Attachments are not supported");
  }

  @Override
  public String storeLocalAttachment(String database, String docId, String attachmentName, String contentType, InputStream input) {
    throw new UnsupportedOperationException("Attachments are not supported");
  }

  @Override
  public Map<String, Object> getStats() {
    return null;
  }

  protected String getElasticSearchIndexNameFromDatabase(String database) {
    String[] pieces = database.split("/", 2);
    if(pieces.length < 2) {
      return database;
    } else {
      return pieces[0];
    }
  }

  protected String getDatabaseNameWithoutUUID(String database) {
    int semicolonIndex = database.indexOf(';');
    if(semicolonIndex >= 0) {
      return database.substring(0, semicolonIndex);
    }
    return database;
  }
}