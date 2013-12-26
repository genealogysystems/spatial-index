package com.findarecord;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;

import com.findarecord.couchbase.CouchbaseBehaviorImpl;
import com.findarecord.couchbase.CouchbaseCAPIBehaviorImpl;
import com.findarecord.neo4j.QueryServer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import org.eclipse.jetty.server.Server;

public class App 
{
  static Logger logger = Logger.getLogger(App.class);
  static int port = 9091;
  static String hostname = "node2.neo4j.far.genealogysystems.com";
  static String username = "Administrator";
  static String password = "1gs234";

  private static GraphDatabaseService graphDb;

  // TODO use Apache Commons CLI Builder to get hostname and port
  public static void main( String[] args )
  {

    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.INFO);

    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("/srv/neo4j");
    registerShutdownHook( graphDb );

    Server server = new Server(8080);

    ServletContextHandler context = new ServletContextHandler(
        ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    context.addServlet(new ServletHolder(new QueryServer(graphDb)),"/");
    server.setHandler(context);

    CouchbaseBehavior couchbaseBehavior = new CouchbaseBehaviorImpl(hostname,port);
    CAPIBehavior capiBehavior = new CouchbaseCAPIBehaviorImpl(16, logger);

    CAPIServer capiServer = new CAPIServer(capiBehavior, couchbaseBehavior, port, username,password);
    try {
      capiServer.start();
      server.start();
      server.join();
    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  private static void registerShutdownHook( final GraphDatabaseService graphDb )
  {
    // Registers a shutdown hook for the Neo4j instance so that it
    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
    // running application).
    Runtime.getRuntime().addShutdownHook( new Thread()
    {
      @Override
      public void run()
      {
        graphDb.shutdown();
      }
    } );
  }
}
