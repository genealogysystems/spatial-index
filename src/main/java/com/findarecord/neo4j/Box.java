package com.findarecord.neo4j;


import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import java.math.BigDecimal;
import java.util.ArrayList;


public class Box {

  //this is the width and height of the box;
  private BigDecimal precision;

  //this is the longitude
  private BigDecimal lon;

  //this is the latitude
  private BigDecimal lat;

  private Polygon polygon;

  private ArrayList<String> ids;

  public Box(BigDecimal lon, BigDecimal lat, BigDecimal precision, ArrayList<String> ids) {
    this.lon = lon;
    this.lat = lat;
    this.precision = precision;
    this.ids = (ArrayList<String>)ids.clone();

    //create polygon

    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(lon.doubleValue(),lat.doubleValue());
    coords[1] = new Coordinate(lon.add(precision).doubleValue(),lat.doubleValue());
    coords[2] = new Coordinate(lon.add(precision).doubleValue(),lat.add(precision).doubleValue());
    coords[3] = new Coordinate(lon.doubleValue(),lat.add(precision).doubleValue());
    coords[4] = new Coordinate(lon.doubleValue(),lat.doubleValue());
    LinearRing ring = new GeometryFactory().createLinearRing(coords);
    polygon = new GeometryFactory().createPolygon(ring,null);


    //add id
    String lonId = lon.toString();
    if(lon.doubleValue() >= 0) lonId = "+"+lonId;

    String latId = lat.toString();
    if(lat.doubleValue() >= 0) latId = "+"+latId;

    this.ids.add(lonId+","+latId);

  }

  public String toString() {
    return lon+","+lat+":"+lon.add(precision)+","+lat.add(precision);
  }

  public Geometry getPolygon() {
    return polygon;
  }

  public BigDecimal getLon() {
    return lon;
  }

  public BigDecimal getLat() {
    return lat;
  }

  public BigDecimal getPrecision() {
    return precision;
  }

  public ArrayList<String> getIds() {
    return ids;
  }

  public String getNodeId() {
    String ret = "";

    for(String id:ids) {
      ret += ":"+id;
    }
    return ret;
  }
}
