package com.findarecord.neo4j;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.geotools.geojson.geom.GeometryJSON;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ValueRepresentation;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.*;

public class HeatmapQuery {

  private GraphDatabaseService graphDb;

  public HeatmapQuery(GraphDatabaseService graphDb) {
    this.graphDb = graphDb;
  }

  public Representation queryPolygon(String geoString, Integer depth) {
    /*
    ArrayList<Representation> t1 = new ArrayList<>();
    t1.add(ValueRepresentation.number(0));
    t1.add(ValueRepresentation.number(1));
    t1.add(ValueRepresentation.number(2));
    Representation t1Rep = new ListRepresentation(RepresentationType.DOUBLE,t1);

    ArrayList<Representation> t2 = new ArrayList<>();
    t2.add(ValueRepresentation.number(3));
    t2.add(ValueRepresentation.number(4));
    t2.add(ValueRepresentation.number(5));
    Representation t2Rep = new ListRepresentation(RepresentationType.DOUBLE,t2);

    ArrayList<Representation> ret = new ArrayList<>();
    ret.add(t1Rep);
    ret.add(t2Rep);

    Representation retRep = new ListRepresentation(RepresentationType.MAP,ret);

    return retRep;
    */

    Representation ret = null;

    //get geometry
    Geometry geometry = geoJSONtoGeometry(geoString);

    //if we have a valid geometry, query it
    if(geometry != null) {
      ret = queryGeometry(geometry, depth);
    }

    return ret;
  }

  private Representation queryGeometry(Geometry geometry, Integer depth) {
    ArrayList<Representation> ret = new ArrayList<>();

    //create bounding envelope
    Envelope envelope = geometry.getEnvelopeInternal();

    //get min and max latitude
    double minLon = envelope.getMinX();
    double maxLon = envelope.getMaxX();
    double minLat = envelope.getMinY();
    double maxLat = envelope.getMaxY();

    //perform query
    try(Transaction tx = graphDb.beginTx()) {

      UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory( graphDb, Settings.NEO_ROOT)
      {
        @Override
        protected void initialize( Node created, Map<String, Object> properties )
        {
          created.setProperty( "id", properties.get( "id" ) );
        }
      };
      Node start = factory.getOrCreate("id", 0);
      //Node start = graphDb.getNodeById(0);
      TraversalDescription traversal = graphDb.traversalDescription()
          .breadthFirst()
          .relationships(DynamicRelationshipType.withName(Settings.NEO_BOX_LINK), Direction.INCOMING)
              //only traverse paths in our bounding box
          .evaluator(getEvaluator(minLon, maxLon, minLat, maxLat, depth));



      for(Path path : traversal.traverse(start)) {

        double hitMinLon = (double) path.lastRelationship().getProperty("minLon");
        double hitMaxLon = (double) path.lastRelationship().getProperty("maxLon");
        double hitMinLat = (double) path.lastRelationship().getProperty("minLat");
        double hitMaxLat = (double) path.lastRelationship().getProperty("maxLat");
        double lon = (hitMinLon+hitMaxLon)/2;
        double lat = (hitMinLat+hitMaxLat)/2;
        int count = (int) path.endNode().getProperty("count");
        long lastUpdated = (long) path.endNode().getProperty("lastUpdated");

        ArrayList<Representation> tmp = new ArrayList<>();
        tmp.add(ValueRepresentation.number(lon));
        tmp.add(ValueRepresentation.number(lat));
        tmp.add(ValueRepresentation.number(count));
        tmp.add(ValueRepresentation.number(lastUpdated));
        ret.add(new ListRepresentation(RepresentationType.DOUBLE,tmp));
      }

      tx.success();
    }

    return new ListRepresentation(RepresentationType.MAP,ret);
  }

  private Evaluator getEvaluator(final double minLon, final double maxLon, final double minLat, final double maxLat, final int depth) {
    return new Evaluator() {
      @Override
      public Evaluation evaluate( final Path path )
      {
        if ( path.length() == 0 )
        {
          return Evaluation.EXCLUDE_AND_CONTINUE;
        }

        boolean insideBoundary = true;

        //if outside our boundary, exclude and prune, else include and continue
        Relationship rel = path.lastRelationship();
        Node node = path.endNode();
        if(rel.isType(DynamicRelationshipType.withName(Settings.NEO_BOX_LINK))
            && (maxLon < (double)rel.getProperty("minLon")
            || maxLat < (double)rel.getProperty("minLat")
            || minLon > (double)rel.getProperty("maxLon")
            || minLat > (double)rel.getProperty("maxLat")
        )
            ) {
          insideBoundary = false;
        }

        if(insideBoundary) {
          //check depth
          if(path.length() < depth) {
            return Evaluation.EXCLUDE_AND_CONTINUE;
          } else if(path.length() == depth) {
            return Evaluation.INCLUDE_AND_PRUNE;
          } else {
            return Evaluation.EXCLUDE_AND_PRUNE;
          }
        } else {
          return Evaluation.EXCLUDE_AND_PRUNE;
        }

      }
    };
  }

  private Geometry geoJSONtoGeometry(String geoString) {
    Geometry geometry;
    GeometryJSON gJSON = new GeometryJSON(15); //15 precision
    Reader reader = new StringReader(geoString);
    try {
      geometry = gJSON.read(reader);

    } catch (IOException e) {
      return null;
    }

    return geometry;
  }

}
