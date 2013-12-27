package com.findarecord.couchbase;

import com.couchbase.capi.CAPIBehavior;

import com.findarecord.neo4j.EntryDelete;
import com.findarecord.neo4j.EntryIndex;
import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import javax.servlet.UnavailableException;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

public class CouchbaseCAPIBehaviorImpl implements CAPIBehavior {

  protected ObjectMapper mapper = new ObjectMapper();
  protected Logger logger;
  protected Semaphore activeRequests;

  private GraphDatabaseService graphDb;

  public CouchbaseCAPIBehaviorImpl(int maxConcurrentRequests, Logger logger, GraphDatabaseService graphDb) {
    this.activeRequests = new Semaphore(maxConcurrentRequests);
    this.logger = logger;
    this.graphDb = graphDb;
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
    try {
      activeRequests.acquire();
    } catch (InterruptedException e) {
      throw new UnavailableException("Too many concurrent requests");
    }

    List<Object> result = new ArrayList<>();

    for (Map<String, Object> doc : docs) {

      // these are the top-level elements that could be in the document sent by Couchbase
      Map<String, Object> meta = (Map<String, Object>)doc.get("meta");
      Map<String, Object> json = (Map<String, Object>)doc.get("json");
      String base64 = (String)doc.get("base64");

      if(meta == null) {
        // if there is no meta-data section, there is nothing we can do
        logger.warn("Document without meta in bulk_docs, ignoring....");
        continue;
      } else {
        if ("non-JSON mode".equals(meta.get("att_reason"))) {
          // optimization, this tells us the body isn't json
          json = new HashMap<>();
        } else {
          if (json == null && base64 != null) {
            // no plain json, let's try parsing the base64 data
            byte[] decodedData = Base64.decodeBase64(base64);
            try {
              // now try to parse the decoded data as json
              json = (Map<String, Object>) mapper.readValue(decodedData, Map.class);
            } catch (IOException e) {
              logger.error("Unable to parse decoded base64 data as JSON, indexing stub for id: " + meta.get("id"));
              logger.error("Body was: " + new String(decodedData) + " Parse error was: " + e);
              json = new HashMap<>();

            }
          }
        }
      }

      // at this point we know we have the document meta-data
      // and the document contents to be indexed are in json

      String id = (String)meta.get("id");
      //String rev = (String)meta.get("rev");

      //ignore checkpoint requests
      if(id.startsWith("_local/")) {
        continue;
      }

      boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;

      if(deleted) {
        //instantiate entry index
        EntryDelete idx = new EntryDelete(graphDb);

        //delete entry
        idx.deleteEntry(id);

      } else {
        Object geojsonObject = json.get("geojson");
        //if geojson is null, continue
        if(geojsonObject == null) {
          //System.out.println("Found null in "+id);
          continue;
        }

        //instantiate entry index
        EntryIndex idx = new EntryIndex(graphDb);

        //index entry
        try ( Transaction tx = graphDb.beginTx() ) {
          result.add(idx.indexEntry(
              id,
              (String) json.get("collection_id"),
              (Integer) json.get("from"),
              (Integer) json.get("to"),
              (ArrayList<String>) json.get("tags"),
              mapper.writeValueAsString(geojsonObject)));
          tx.success();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      // TODO only add response if we actually saved things
      Map<String, Object> itemResponse = new HashMap<>();
      itemResponse.put("id", id);
      itemResponse.put("rev", null); //not sure why null works here...
      result.add(itemResponse);

    }

    activeRequests.release();

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