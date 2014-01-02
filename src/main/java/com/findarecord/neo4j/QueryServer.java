package com.findarecord.neo4j;


import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.StringLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServer extends HttpServlet {

  private static final Logger logger = LoggerFactory.getLogger(QueryServer.class);
  protected ObjectMapper mapper = new ObjectMapper();

  private GraphDatabaseService graphDb;

  public QueryServer(GraphDatabaseService graphDb) {
    this.graphDb = graphDb;
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    resp.setContentType("application/json;charset=utf-8");

    String uri = req.getRequestURI();
    String[] splitUri = getUriPieces(uri);

    if (splitUri[0].equals("cypher")) {
      handleCypherQuery(req, resp);
      return;
    }

    if (splitUri[0].equals("distance")) {
      handleDistanceQuery(req, resp);
      return;
    }

    if (splitUri[0].equals("shape")) {
      handleShapeQuery(req, resp);
      return;
    }

    resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
  }

  protected void handleCypherQuery(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException{

    if(!req.getMethod().equals("POST")) {
      throw new UnsupportedOperationException("POST required");
    }

    Map<String, Object> parsedValue = getJSON(req);

    //logger.warn("obj is {}", parsedValue.get("query"));

    Map<String, Object> responseMap = new HashMap();
    responseMap.put("ok", false);

    Map<String, Object> resultMap = new HashMap();

    ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);

    try ( Transaction tx = graphDb.beginTx() ) {

      ExecutionResult result = engine.execute((String) parsedValue.get("query"));
      responseMap.put("ok", true);
      responseMap.put("result", result.dumpToString());
      tx.success();
    }

    OutputStream os = resp.getOutputStream();
    resp.setStatus(HttpServletResponse.SC_OK);
    mapper.writeValue(os, responseMap);
  }

  protected void handleDistanceQuery(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException{

    Map<String, Object> json = getJSON(req);

    HashMap<String,String> mapping = new HashMap<>();
    mapping.put("lon","double");
    mapping.put("lat","double");
    mapping.put("radius","double");
    mapping.put("from","int");
    mapping.put("to","int");
    mapping.put("tags","arraylist");
    mapping.put("depth","int");
    mapping.put("count","int");
    mapping.put("offset","int");

    HashMap<String,Object> params;

    try {
      params = extractParams(mapping, json);
    } catch (Exception e) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      PrintWriter writer = resp.getWriter();
      writer.print("{\"error\":\""+e.getMessage()+"\"}");
      return;
    }

    EntryQuery idx = new EntryQuery(graphDb);

    ArrayList<String> results = idx.queryDistance(
        (Double) params.get("lon"),
        (Double) params.get("lat"),
        (Double) params.get("radius"),
        (Integer) params.get("from"),
        (Integer) params.get("to"),
        (ArrayList<String>) params.get("tags"),
        (Integer) params.get("depth"),
        (Integer) params.get("count"),
        (Integer) params.get("offset"));

    OutputStream os = resp.getOutputStream();
    resp.setStatus(HttpServletResponse.SC_OK);
    mapper.writeValue(os, results);
  }

  protected void handleShapeQuery(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException{

    Map<String, Object> json = getJSON(req);

    HashMap<String,String> mapping = new HashMap<>();
    mapping.put("geojson","string");
    mapping.put("from","int");
    mapping.put("to","int");
    mapping.put("tags","arraylist");
    mapping.put("depth","int");
    mapping.put("count","int");
    mapping.put("offset","int");

    HashMap<String,Object> params;

    try {
      params = extractParams(mapping, json);
    } catch (Exception e) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      PrintWriter writer = resp.getWriter();
      writer.print("{\"error\":\""+e.getMessage()+"\"}");
      return;
    }

    EntryQuery idx = new EntryQuery(graphDb);

    ArrayList<String> results = idx.queryPolygon(
        (String) params.get("geojson"),
        (Integer) params.get("from"),
        (Integer) params.get("to"),
        (ArrayList<String>) params.get("tags"),
        (Integer) params.get("depth"),
        (Integer) params.get("count"),
        (Integer) params.get("offset"));

    OutputStream os = resp.getOutputStream();
    resp.setStatus(HttpServletResponse.SC_OK);
    mapper.writeValue(os, results);
  }

  Map<String, Object> getJSON(HttpServletRequest req)
      throws ServletException, IOException{

    InputStream is = req.getInputStream();
    int requestLength = req.getContentLength();
    byte[] buffer = new byte[requestLength];
    IOUtils.readFully(is, buffer, 0, requestLength);

    return (Map<String, Object>) mapper.readValue(buffer, Map.class);
  }

  HashMap<String,Object> extractParams(HashMap<String,String> mapping, Map<String, Object> json) throws Exception {

    HashMap<String,Object> params = new HashMap<>();
    Object temp;

    for (Map.Entry<String, String> entry : mapping.entrySet()) {
      temp = json.get(entry.getKey());

      if(temp == null) {
        throw new Exception(entry.getKey() + " must be of type " + entry.getValue());
      }

      switch (entry.getValue()) {
        case "double":
          if(temp.getClass().equals(Double.class)) {
           params.put(entry.getKey(),temp);
          } else if(temp.getClass().equals(Integer.class)) {
           params.put(entry.getKey(),((Integer)temp).doubleValue());
          } else {
           throw new Exception(entry.getKey() + " must be of type " + entry.getValue());
          }
          break;
        case "int":
          if(temp.getClass().equals(Integer.class)) {
            params.put(entry.getKey(),temp);
          } else {
            throw new Exception(entry.getKey() + " must be of type " + entry.getValue());
          }
          break;
        case "string":
          if(temp.getClass().equals(String.class)) {
            params.put(entry.getKey(),temp);
          } else {
            throw new Exception(entry.getKey() + " must be of type " + entry.getValue());
          }
          break;
        case "arraylist":
          if(temp.getClass().equals(ArrayList.class)) {
            params.put(entry.getKey(),temp);
          } else {
            throw new Exception(entry.getKey() + " must be of type " + entry.getValue());
          }
          break;
        default:throw new Exception("Unknown type " + entry.getValue());
      }
    }

    return params;
  }

  String[] getUriPieces(String uri) {
    // remove initial /
    if (uri.startsWith("/")) {
      uri = uri.substring(1);
    }
    String[] result = uri.split("/");
    return result;
  }
}
