/*
 * Author: Joshua Jansen Van Vuren
 * Date: 26 Nov 2019
 * Desc: Unit Tests for Utility Functions
 */

package org.ambrite.josh;

import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.junit.Assert;

import org.ambrite.josh.Utils;

/* Tests of bioStats */
@RunWith(JUnit4.class)
public class UtilsTest {
    // static input data
    static ArrayList<String> invalidName = new ArrayList<>(Arrays.asList("Hi.", "M", "45", "60", "154"));
    static ArrayList<String> invalidSex = new ArrayList<>(Arrays.asList("Fred", "Male", "45", "60", "154"));
    static ArrayList<String> invalidAge = new ArrayList<>(Arrays.asList("Fred", "M", "Fourty", "60", "154"));
    static ArrayList<String> invalidWeight = new ArrayList<>(Arrays.asList("Fred", "F", "40", "-60", "154"));
    static ArrayList<String> invalidHeight = new ArrayList<>(Arrays.asList("Fred", "M", "40", "-60", "5000"));
    static ArrayList<String> valid = new ArrayList<>(Arrays.asList("Fred", "M", "40", "60", "150"));

    // ********************************
    // ** test the record validifier **
    // ********************************
    @Test
    public void testInvalidName() throws Exception {
        boolean ret = Utils.ensureInputStringValid(invalidName);
        
        Assert.assertEquals(false, ret);
    }

    @Test
    public void testInvalidSex() throws Exception {
        boolean ret = Utils.ensureInputStringValid(invalidSex);

        Assert.assertEquals(false, ret);
    }

    @Test
    public void testInvalidAge() throws Exception {
        boolean ret = Utils.ensureInputStringValid(invalidAge);

        Assert.assertEquals(false, ret);
    }

    @Test
    public void testInvalidWeight() throws Exception {
        boolean ret = Utils.ensureInputStringValid(invalidWeight);

        Assert.assertEquals(false, ret);
    }

    @Test
    public void testInvalidHeight() throws Exception {
        boolean ret = Utils.ensureInputStringValid(invalidHeight);

        Assert.assertEquals(false, ret);
    }

    @Test
    public void testValid() throws Exception {
        boolean ret = Utils.ensureInputStringValid(valid);

        Assert.assertEquals(true, ret);
    }
}