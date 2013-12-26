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
import java.util.HashMap;
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

    resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
  }

  protected void handleCypherQuery(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException{

    if(!req.getMethod().equals("POST")) {
      throw new UnsupportedOperationException("POST required");
    }

    InputStream is = req.getInputStream();

    int requestLength = req.getContentLength();
    byte[] buffer = new byte[requestLength];
    IOUtils.readFully(is, buffer, 0, requestLength);

    @SuppressWarnings("unchecked")
    Map<String, Object> parsedValue = (Map<String, Object>) mapper
        .readValue(buffer, Map.class);

    logger.warn("obj is {}", parsedValue.get("query"));

    Map<String, Object> responseMap = new HashMap();
    responseMap.put("ok", false);

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
    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().println("{\"distance\":true}");
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
