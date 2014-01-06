package com.findarecord.neo4j;

import com.vividsolutions.jts.geom.*;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.GeodeticCalculator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntryQuery {

  private static final Logger logger = LoggerFactory.getLogger(QueryServer.class);
  private GraphDatabaseService graphDb;

  public EntryQuery(GraphDatabaseService graphDb) {
    this.graphDb = graphDb;
  }

  public ArrayList<String> queryPolygon(String geoString, Integer from, Integer to, ArrayList<String> tags, Integer depth, Integer count, Integer offset) {
    ArrayList<String> entryIDs = new ArrayList<>();

    //get geometry
    Geometry geometry = geoJSONtoGeometry(geoString);

    //if we have a valid geometry, query it
    if(geometry != null) {
      entryIDs = queryGeometry(geometry, from, to, tags, depth, count, offset);
    }

    return entryIDs;
  }

  public ArrayList<String> queryDistance(double lon, double lat, double radius, Integer from, Integer to, ArrayList<String> tags, Integer depth, Integer count, Integer offset) {
    ArrayList<String> entryIDs;

    //create calculator to get/set the radius correctly
    GeodeticCalculator calc = new GeodeticCalculator();
    calc.setStartingGeographicPoint(lon, lat);

    //create circle
    int SIDES = 32;
    double baseAzimuth = 360.0 / SIDES;
    Coordinate coords[] = new Coordinate[SIDES+1];
    for( int i = 0; i < SIDES; i++){
      double azimuth = 180 - (i * baseAzimuth);
      calc.setDirection(azimuth, radius*1000);
      Point2D point = calc.getDestinationGeographicPoint();
      coords[i] = new Coordinate(point.getX(), point.getY());
    }
    coords[SIDES] = coords[0];
    LinearRing ring = new GeometryFactory().createLinearRing(coords);
    Polygon circle = new GeometryFactory().createPolygon( ring, null );

    //perform query
    entryIDs = queryGeometry(circle, from, to, tags, depth, count, offset);

    Envelope envelope = circle.getEnvelopeInternal();

    //entryIDs.add(envelope.getMinX()+","+envelope.getMinY()+"::"+envelope.getMaxX()+","+envelope.getMaxY());

    return entryIDs;
  }

  private ArrayList<String> queryGeometry(Geometry geometry, Integer from, Integer to, ArrayList<String> tags, Integer depth, Integer count, Integer offset) {
    ArrayList<String> entryIDs = new ArrayList<>();

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
          .relationships(DynamicRelationshipType.withName(Settings.NEO_BOX_LINK),Direction.INCOMING)
          .relationships(DynamicRelationshipType.withName(Settings.NEO_BOX_INTERSECT),Direction.OUTGOING)
              //only traverse paths in our bounding box
          .evaluator(getEvaluator(minLon, maxLon, minLat, maxLat, from, to, new HashSet<>(tags), depth))
              //only return entries
          .evaluator(Evaluators.includeWhereLastRelationshipTypeIs(DynamicRelationshipType.withName(Settings.NEO_BOX_INTERSECT)));

      List<Node> hits = new ArrayList<>();

      for(Path path : traversal.traverse(start)) {
        hits.add(path.endNode());
      }

      Collections.sort(hits, getComparator(from, to, geometry.getCentroid()));

      int i = 0;
      int end = offset+count;
      HashSet<String> collectionIds = new HashSet<>();
      String collectionId;
      //loop through our results
      for(Node entry: hits) {

        //make sure we haven't seen this collection before
        collectionId = (String) entry.getProperty("collection_id");
        if(!collectionIds.contains(collectionId)) {
          //add collection we have seen to our hashset
          collectionIds.add(collectionId);

          //if we are in range of our query, add to result
          if(i >= offset && i < end) {
            entryIDs.add(collectionId);
          }
          i++;
        }
      }
      tx.success();
    }

    return entryIDs;
  }

  private Comparator<Node> getComparator(final Integer from, final Integer to, final Geometry centroid) {
    return new Comparator<Node>() {
      @Override
      public int compare(Node node1, Node node2) {

        //compare distances
        double node1Distance = getDistance(node1,centroid);
        double node2Distance = getDistance(node2,centroid);

        if(node1Distance < node2Distance) {
          return -1;
        }
        if(node1Distance > node2Distance) {
          return 1;
        }

        //compare from and to dates
        int node1Size = getDateRange(node1, from, to);
        int node2Size = getDateRange(node2, from, to);

        //sort sorts in ascending order
        //we want the larger of the 2 to be sorted first
        //so we return -1 if node1 is larger than node2
        if(node1Size > node2Size) {
          return -1;
        }
        if(node1Size < node2Size) {
          return 1;
        }

        if(node1.getId()>node2.getId()) {
          return -1;
        } else {
          return 1;
        }
      }
    };
  }

  public double getDistance(Node node, Geometry centroid) {
    double distance = Double.MAX_VALUE;

    //get array of lats and lons
    double[] lons = (double[]) node.getProperty("lons");
    double[] lats = (double[]) node.getProperty("lats");
    double temp;

    //loop through and set the lowest distance
    for(int i=0;i<lats.length;i++) {
      Coordinate coord = new Coordinate(lons[i], lats[i]);
      temp = centroid.distance(new GeometryFactory().createPoint(coord));
      if(temp < distance) {
        distance = temp;
      }
    }

    return distance;
  }

  private int getDateRange(Node node, Integer from, Integer to) {
    Integer nodeFrom = (Integer)node.getProperty("from");
    Integer nodeTo = (Integer)node.getProperty("to");
    int nodeSize;
    if(nodeFrom < from && nodeTo > to) {
      nodeSize = from - to;
      //if node from overlaps
    } else if(nodeFrom < from) {
      nodeSize = nodeTo - from;
      //if node to overlaps
    }else if(nodeTo > to) {
      nodeSize = to - nodeFrom;
      //if node is encompassed by our range
    } else {
      nodeSize = nodeTo - nodeFrom;
    }


    return nodeSize;
  }

  private Evaluator getEvaluator(final double minLon, final double maxLon, final double minLat, final double maxLat,final int from, final int to, final Set<String> tags, final Integer depth) {
    return new Evaluator() {
      @Override
      public Evaluation evaluate( final Path path )
      {
        if ( path.length() == 0 )
        {
          return Evaluation.EXCLUDE_AND_CONTINUE;
        }

        boolean includeAndContinue = true;

        //if we are at maximum depth
        if(path.length() > depth) {
          return Evaluation.EXCLUDE_AND_PRUNE;
        }

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
          includeAndContinue = false;
        }
        //if(rel.isType(DynamicRelationshipType.withName(Settings.NEO_BOX_INTERSECT))) {
        if(node.hasLabel(DynamicLabel.label( "Entry" ))) {
          boolean hasTags = false;

          //if we were passed in no tags, don't check them
          if(tags.size() == 0) {
            hasTags = true;
            //else loop through the node's tags and make sure they match
          } else {
            String[] nodeTags = (String[])node.getProperty("tags");
            for(String nodeTag:nodeTags) {
              if(tags.contains(nodeTag)) {
                hasTags = true;
              }
            }
          }

          if(from > (int)node.getProperty("to")
              || to < (int)node.getProperty("from")
              || !hasTags) {
            includeAndContinue = false;
          }
        }


        if(includeAndContinue) {
          return Evaluation.INCLUDE_AND_CONTINUE;
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