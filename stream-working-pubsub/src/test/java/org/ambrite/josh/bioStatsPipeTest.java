package org.ambrite.josh;

import java.util.Arrays;
import java.util.List;

import org.ambrite.josh.Person;
import org.ambrite.josh.Constants.ThreeState;
import org.ambrite.josh.BioStatsPipe.StringsToPeople;
import org.ambrite.josh.BioStatsPipe.PartitionValidRecords;
import org.ambrite.josh.BioStatsPipe.toStringForOutput;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

//Static inputs for tests

/* Tests of bioStats */
@RunWith(JUnit4.class)
public class bioStatsPipeTest {

	@Rule public TestPipeline p = TestPipeline.create();

	// create static input data
	static final List<String> LINES = Arrays.asList(
		" \"Alex\",       \"M\",   41,       74,      170",
		"\"Hank\",       \"M\",   ,       71,      158");

	// create expected output person
	static final Person alexValid = new Person("\"Alex\"","\"M\"",41,74,170);
	static final Person hankInvalid = new Person("\"Hank\"","\"M\"",0,71,158);
	static final Person alexMinor = new Person("\"Alex\"","\"M\"",41,74,170,ThreeState.FALSE);
	static final Person hankMinor = new Person("\"Hank\"","\"M\"",0,71,158,ThreeState.UNSET);
	String outputString = "\"Alex\", \"M\", 41, 74, 170, false";
	// *********************************************
	// ** Unit test for split reconds into people **
	// *********************************************
	@Test
	public void testSplitRecords() throws Exception {
		PCollection<String> input = p.apply(Create.of(LINES));
		
		// apply the transform
		PCollection<Person> output = input.apply(new StringsToPeople());

		// validate the outputs
		PAssert.that(output).containsInAnyOrder(alexMinor,hankMinor);

		p.run().waitUntilFinish();
	}

	@Test
	public void testParitionValidRecords() throws Exception {
		// note 1 is invalid and 0 is valid for the partition ID's
		PCollection<String> input = p.apply(Create.of(LINES));
		PCollection<Person> people = input.apply(new StringsToPeople());

		// apply the transform
		PCollectionList<Person> validityOfRecords =
		people.apply(Partition.of(2, new PartitionValidRecords()));

		// validate outputs
		PAssert.that(validityOfRecords.get(1)).containsInAnyOrder(hankMinor);
		PAssert.that(validityOfRecords.get(0)).containsInAnyOrder(alexMinor);

		p.run().waitUntilFinish();
	}

	@Test
	public void testToStringForOutput() throws Exception {
		// create input
		PCollection<Person> input = p.apply(Create.of(alexMinor));

		// create output string
		PCollection<String> outputAlex = input.apply(ParDo.of(new toStringForOutput()));

		// validate outputs
		PAssert.that(outputAlex).containsInAnyOrder(outputString);

		p.run().waitUntilFinish();
	}
}