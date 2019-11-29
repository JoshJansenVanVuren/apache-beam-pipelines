/*
 * Author: Joshua Jansen Van Vuren
 * Date: 26 Nov 2019
 * Desc: Unit Tests For Batch Pipeline
 */

package org.ambrite.josh;

import java.util.Arrays;
import java.util.List;

import org.ambrite.josh.Person;
import org.ambrite.josh.Constants.ThreeState;
import org.ambrite.josh.BioStatsPipe.StringsToPeople;
import org.ambrite.josh.BioStatsPipe.toStringForOutput;
import org.ambrite.josh.BioStatsPipe.tagValidAndInvalidRecords;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/* Tests of bioStats */
@RunWith(JUnit4.class)
public class BioStatsPipeTest {

	@Rule
	public TestPipeline p = TestPipeline.create();

	// create static input data
	static final List<String> LINES = Arrays.asList(" Alex,       M,   41,       74,      170",
			"Hank,       M,   ,       71,      158");

	// create expected output person
	static final Person alexMinor = new Person("Alex", "M", 41, 74, 170, ThreeState.FALSE);
	String validOutputString = "Alex, M, 41, 74, 170, false";
	String invalidOutputString = "Hank,       M,   ,       71,      158";

	// *********************************************
	// ** Unit test for split reconds into people **
	// *********************************************
	@Test
	public void testSplitRecords() throws Exception {
		PCollection<String> input = p.apply(Create.of(validOutputString));

		// apply the transform
		PCollection<Person> output = input.apply(new StringsToPeople());

		// validate the outputs
		PAssert.that(output).containsInAnyOrder(alexMinor);

		p.run().waitUntilFinish();
	}

	@Test
	public void testTagValidAndInvalidRecords() throws Exception {
		PCollection<String> input = p.apply(Create.of(LINES));

		// apply the tagging pardo
		PCollectionTuple mixedCollection = input.apply(ParDo.of(new tagValidAndInvalidRecords())
				.withOutputTags(Constants.validRecordTag, TupleTagList.of(Constants.invalidRecordTag)));

		// Get subset of the output with tag validRecordTag.
		PCollection<String> valid = mixedCollection.get(Constants.validRecordTag);

		// Get subset of the output with tag invalidRecordTag.
		PCollection<String> invalid = mixedCollection.get(Constants.invalidRecordTag);

		// transform to person
		PCollection<Person> people = valid.apply(new StringsToPeople());

		// validate outputs
		PAssert.that(people).containsInAnyOrder(alexMinor);
		PAssert.that(invalid).containsInAnyOrder(invalidOutputString);

		p.run();
	}

	@Test
	public void testToStringForOutput() throws Exception {
		// create input
		PCollection<Person> input = p.apply(Create.of(alexMinor));

		// create output string
		PCollection<String> outputAlex = input.apply(ParDo.of(new toStringForOutput()));

		// validate outputs
		PAssert.that(outputAlex).containsInAnyOrder(validOutputString);

		p.run().waitUntilFinish();
	}
}