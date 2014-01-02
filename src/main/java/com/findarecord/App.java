package com.findarecord;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;

import com.findarecord.couchbase.CouchbaseBehaviorImpl;
import com.findarecord.couchbase.CouchbaseCAPIBehaviorImpl;
import com.findarecord.neo4j.QueryServer;
import org.apache.commons.cli.*;
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
  static int app_port = 8080;
  static int xdcr_port = 9091;
  static String hostname = "node1.neo4j.far.genealogysystems.com";
  static String username = "Administrator";
  static String password = "1gs234";
  static int concurrency = 8;
  static String neo4j_dir;

  private static GraphDatabaseService graphDb;

  // TODO use Apache Commons CLI Builder to get hostname and port
  public static void main( String[] args )
  {
    CommandLineParser parser = new PosixParser();
    HelpFormatter help = new HelpFormatter();
    CommandLine cmd = null;
    Options options = buildOptions();
    //Parameters p = null;


    try {
      cmd = parser.parse(options, args);

      neo4j_dir = (String) cmd.getParsedOptionValue("neo4j_dir");

      if(cmd.hasOption("app_port")) {
        app_port = ((Number)cmd.getParsedOptionValue("app_port")).intValue();
      }
      if(cmd.hasOption("xdcr_port")) {
        xdcr_port = ((Number)cmd.getParsedOptionValue("xdcr_port")).intValue();
      }
      if(cmd.hasOption("hostname")) {
        hostname = (String) cmd.getParsedOptionValue("hostname");
      }
      if(cmd.hasOption("username")) {
        username = (String) cmd.getParsedOptionValue("username");
      }
      if(cmd.hasOption("password")) {
        password = (String) cmd.getParsedOptionValue("password");
      }
      if(cmd.hasOption("concurrency")) {
        concurrency = ((Number)cmd.getParsedOptionValue("concurrency")).intValue();
      }
    } catch (ParseException e) {
      System.err.println("Wrong parameters:" + e.getMessage());
      help.printHelp("spatial-index", options);
      System.exit(1);
    }

    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.INFO);

    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(neo4j_dir);
    registerShutdownHook( graphDb );

    Server server = new Server(app_port);

    ServletContextHandler context = new ServletContextHandler(
        ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    context.addServlet(new ServletHolder(new QueryServer(graphDb)),"/");
    server.setHandler(context);

    CouchbaseBehavior couchbaseBehavior = new CouchbaseBehaviorImpl(hostname,xdcr_port);
    CAPIBehavior capiBehavior = new CouchbaseCAPIBehaviorImpl(concurrency, logger, graphDb);

    CAPIServer capiServer = new CAPIServer(capiBehavior, couchbaseBehavior, xdcr_port, username,password);
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

  @SuppressWarnings("static-access")
  protected static Options buildOptions() {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("neo4j_dir")
        .hasArg()
        .isRequired()
        .withType(String.class)
        .withDescription("The neo4j data directory")
        .create("neo4j_dir"));

    options.addOption(OptionBuilder.withArgName("app_port")
        .hasArg()
        .withType(Number.class)
        .withDescription("The port the app will run on - default 8080")
        .create("app_port"));

    options.addOption(OptionBuilder.withArgName("xdcr_port")
        .hasArg()
        .withType(Number.class)
        .withDescription("The port xdcr will run on - default 9091")
        .create("xdcr_port"));

    options.addOption(OptionBuilder.withArgName("hostname")
        .hasArg()
        .withType(String.class)
        .withDescription("The hostname - default node1.neo4j.far.genealogysystems.com")
        .create("hostname"));

    options.addOption(OptionBuilder.withArgName("username")
        .hasArg()
        .withType(String.class)
        .withDescription("The XDCR username - default Administrator")
        .create("username"));

    options.addOption(OptionBuilder.withArgName("password")
        .hasArg()
        .withType(String.class)
        .withDescription("The port the app will run on - default (see source code)")
        .create("password"));

    options.addOption(OptionBuilder.withArgName("concurrency")
        .hasArg()
        .withType(Number.class)
        .withDescription("The XDCR concurrency - default 8")
        .create("concurrency"));

    return options;
  }
}
