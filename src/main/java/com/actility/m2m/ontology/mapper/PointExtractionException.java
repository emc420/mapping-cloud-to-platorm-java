package com.actility.m2m.ontology.mapper;

public class PointExtractionException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    public final String pointName;

    public PointExtractionException(String errorMessage, String pointName) {
        super(errorMessage);
        this.pointName = pointName;
    }
}
