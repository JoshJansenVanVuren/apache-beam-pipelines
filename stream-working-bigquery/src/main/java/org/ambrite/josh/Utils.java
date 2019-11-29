/*
 * Author: Joshua Jansen Van Vuren
 * Date: 26 Nov 2019
 * Desc: Encompasses the odds and ends of the class
 */

package org.ambrite.josh;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ambrite.josh.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils implements Serializable {
    private static final long serialVersionUID = 1L;

    // declare the logger
	private static final Logger LOG = LoggerFactory.getLogger(BioStatsPipe.class);

    // ************************************************************
    // ** checks various criteria for the input data to be valid **
    // ************************************************************
    public static boolean ensureInputStringValid(ArrayList<String> in) {
        boolean valid = true;
        // ensure correct number of fields      
        if (in.size() != Constants.NUM_PERSON_MEMBERS) {
            valid = false;
            LOG.error("Mismatch number of fields for person\n\n" + in);
        }

        // ensure name is valid
        Pattern namePat = Pattern.compile("[^a-z0-9 ]", Pattern.CASE_INSENSITIVE);
        Matcher nameMatch = namePat.matcher(in.get(Constants.NAME_INDEX));
        if(nameMatch.find()) {
            valid = false;
            LOG.error("Name contains special characters\n\n" + in);
        }

        // ensure sex is valid
        Pattern sexPat = Pattern.compile("^[M|F]$");
        Matcher sexMatch = sexPat.matcher(in.get(Constants.SEX_INDEX));
        if(!sexMatch.find()) {
            valid = false;
            LOG.error("Sex is in the incorrect format\n\n" + in);
        }

        // ensure age is valid
        Pattern intPat = Pattern.compile("^[0-9]{1,3}$");
        Matcher ageMatch = intPat.matcher(in.get(Constants.AGE_INDEX));
        if(!ageMatch.find()) {
            valid = false;
            LOG.error("Age is in the incorrect format\n\n" + in);
        }

        // ensure weight is valid
        Matcher weightMatch = intPat.matcher(in.get(Constants.WEIGHT_INDEX));
        if(!weightMatch.find()) {
            valid = false;
            LOG.error("Weight is in the incorrect format\n\n" + in);
        }

        // ensure height is valid
        Matcher heightMatch = intPat.matcher(in.get(Constants.HEIGHT_INDEX));
        if(!heightMatch.find()) {
            valid = false;
            LOG.error("Height is in the incorrect format\n\n" + in);
        }

        return valid;
    }
}