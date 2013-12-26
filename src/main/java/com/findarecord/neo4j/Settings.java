package com.findarecord.neo4j;

import java.math.BigDecimal;

public class Settings {

  private Settings(){}

  public final static BigDecimal DEPTH = new BigDecimal(0.001);
  public final static int DECIMALS = 3;
  public final static BigDecimal WIDTH = new BigDecimal(10);

  public final static String NEO_ROOT = "ROOT";
  public final static String NEO_BOX = "BOX";
  public final static String NEO_BOX_LINK = "BOX_LINK";
  public final static String NEO_BOX_LINK_INDEX = "BOX_LINK_INDEX";
  public final static String NEO_BOX_INTERSECT = "BOX_INTERSECT";
  public final static String NEO_ENTRY = "ENTRY";

}
