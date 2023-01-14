package com.mas2022datascience.generalworkeracceleration.processor;

public enum AccelerationTypes {
  SPRINT("SPRINT"), SHORTACCELERATION("SHORTACCELERATION"), INCREMENTALRUN("INCREMENTALRUN"), JOG("JOG");

  final private String abbreviation;

  AccelerationTypes(String abbreviation) {
    this.abbreviation = abbreviation;
  }

  public String getAbbreviation() {
    return abbreviation;
  }
}
