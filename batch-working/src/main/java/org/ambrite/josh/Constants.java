/*
 * Author: Joshua Jansen Van Vuren
 * Date: 26 Nov 2019
 * Desc: Constants associated with pipeline
 */

package org.ambrite.josh;

public class Constants {
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

    public enum ThreeState {
        TRUE, FALSE, UNSET
    };
}