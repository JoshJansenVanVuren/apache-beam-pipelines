/*
 * Author: Joshua Jansen Van Vuren
 * Date: 26 Nov 2019
 * Desc: Constants associated with pipeline
 */

package org.ambrite.josh;

import java.io.Serializable;

import org.apache.beam.sdk.values.TupleTag;

public class Constants implements Serializable {
    private static final long serialVersionUID = 1L;

    // ******************************************
    // ** if the pipeline is in debugging mode **
    // ******************************************
    public static final boolean DEBUGGING_MODE = false;


    // ****************************************************
    // ** all constants associated with the person class **
    // ****************************************************
    public static final int NAME_INDEX = 0;
    public static final int SEX_INDEX = 1;
    public static final int AGE_INDEX = 2;
    public static final int WEIGHT_INDEX = 3;
    public static final int HEIGHT_INDEX = 4;
    public static final int NUM_PERSON_MEMBERS = 5;
    public static final char DELIMITER = ',';

    public static final int AGE_OF_MINOR = 18;

    public enum ThreeState {
        TRUE, FALSE, UNSET
    };

    // ********************************************************
    // ** define two TupleTags for valid and invalid recrods **
    // ********************************************************
	public final static TupleTag<String> validRecordTag = new TupleTag<String>() {
		private static final long serialVersionUID = 1L;
	};

	public final static TupleTag<String> invalidRecordTag = new TupleTag<String>() {
		private static final long serialVersionUID = 1L;
    };
    
    // **************************************************
    // ** constant to define the state of the pipeline **
    // **************************************************
    public enum State {
        BATCH_LOCAL, STREAMING_PUBSUB, STREAMING_BIGQUERY
    };

    public final static State state = State.STREAMING_BIGQUERY;
}