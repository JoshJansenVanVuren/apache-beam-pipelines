package org.ambrite.josh;

import org.ambrite.josh.Constants;
import org.ambrite.josh.Constants.ThreeState;
import java.util.ArrayList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BioStatsPipe {

	// declare the logger
	private static final Logger LOG = LoggerFactory.getLogger(BioStatsPipe.class);

	// ****************************************************************
	// ** transform for conversion between String and Person objects **
	// ****************************************************************
	public static class StringsToPeople extends PTransform<PCollection<String>, PCollection<Person>> {
		private static final long serialVersionUID = 1L;

		@Override
		public PCollection<Person> expand(PCollection<String> lines) {
			// convert lines of text into people objects
			PCollection<Person> split = lines.apply(ParDo.of(new DoFn<String, Person>() {
				private static final long serialVersionUID = 1L;

				@ProcessElement
				public void processElement(@Element String in, OutputReceiver<Person> out) {
					ArrayList<String> members = new ArrayList<String>();

					// split the string into an array list
					int subStrStart = 0;
					for (int i = 0; i < in.length(); i++) {
						// ignore whitespace before start
						if (i > 0 && in.charAt(i - 1) == ' ' && in.charAt(i) != ' ') {
							subStrStart = i;
						}

						if (in.charAt(i) == ',') {
							members.add(in.substring(subStrStart, i));
							subStrStart = i + 1;
						} // if
					} // for

					// split the last member in the list
					members.add(in.substring(subStrStart, in.length()));

					// if input string is not valid LOG and drop data
					if (!Utils.ensureInputStringValid(members))
						return;

					// verify the "is minor" field
					ThreeState minor = ThreeState.UNSET;
					if (StringUtils.isNotBlank(members.get(Constants.AGE_INDEX))) {
						if (Integer.parseInt(members.get(Constants.AGE_INDEX)) < 18) {
							minor = ThreeState.TRUE;
						} else {
							minor = ThreeState.FALSE;
						} // else
					} // if

					LOG.info(members.toString());
					// log if incorrect number of fields
					if (members.size() != Constants.NUM_PERSON_MEMBERS)
						LOG.error("\n\nInput String has incorrect number of members" + members.toString() + "\n\n");

					// create new person
					Person personTemp = new Person(members.get(Constants.NAME_INDEX), members.get(Constants.SEX_INDEX),
							StringUtils.isNotBlank(members.get(Constants.AGE_INDEX))
									? Integer.parseInt(members.get(Constants.AGE_INDEX))
									: 0,
							StringUtils.isNotBlank(members.get(Constants.WEIGHT_INDEX))
									? Integer.parseInt(members.get(Constants.WEIGHT_INDEX))
									: 0,
							StringUtils.isNotBlank(members.get(Constants.HEIGHT_INDEX))
									? Integer.parseInt(members.get(Constants.HEIGHT_INDEX))
									: 0,
							minor);

					out.output(personTemp);
				}
			}));
			return split;
		}
	} // class

	// ********************************************
	// ** Paritition of valid and invalid fields **
	// ********************************************
	public static class PartitionValidRecords implements PartitionFn<Person> {
		private static final long serialVersionUID = 1L;

		@Override
		public int partitionFor(Person person, int numPartitions) {
			boolean flag = false;
			if (person.getName().isEmpty())
				flag = true;
			if (person.getSex().isEmpty())
				flag = true;
			if (person.getAge() == 0)
				flag = true;
			if (person.getHeight() == 0)
				flag = true;
			if (person.getWeight() == 0)
				flag = true;

			// if any empty then add to list
			if (flag) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	// *****************************************************************************
	// ** DoFn to perform a toString to prepare PColletion<Person> for txt output **
	// *****************************************************************************
	static class toStringForOutput extends DoFn<Person, String> {
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(@Element final Person in, final OutputReceiver<String> out) {
			out.output(in.toString());
		}
	}

	// ******************************************************************************
	// ** DoFn to perform a toNiceString to prepare PColletion<Person> for logging
	// **
	// ******************************************************************************
	static class outputPersonData extends DoFn<Person, Person> {
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(@Element final Person in, final OutputReceiver<Person> out) {
			LOG.info(in.toNiceString());
		}
	}

	// *************************
	// ** gs pipeline options **
	// *************************
	public interface BioStatsOptions extends ExampleOptions, StreamingOptions {
		@Description("The Cloud Pub/Sub topic to read from.")
		@Required
		ValueProvider<String> getInputTopic();

		void setInputTopic(ValueProvider<String> value);

		@Description("The Cloud Pub/Sub topic to publish to. " + "The name should be in the format of "
				+ "projects/<project-id>/topics/<topic-name>.")
		@Validation.Required
		ValueProvider<String> getValidOutputTopic();

		void setValidOutputTopic(ValueProvider<String> outputTopic);

		@Description("The Cloud Pub/Sub topic to publish to. " + "The name should be in the format of "
				+ "projects/<project-id>/topics/<topic-name>.")
		@Validation.Required
		ValueProvider<String> getInvalidOutputTopic();

		void setInvalidOutputTopic(ValueProvider<String> outputTopic);

	}

	static void runBioStats(BioStatsOptions options) {
		// create pipleline
		Pipeline p = Pipeline.create(options);

		options.setStreaming(true);

		PCollection<String> lines = p.apply("Read PubSub Events",
				PubsubIO.readStrings().fromTopic(options.getInputTopic()));

		// PCollection<String> lines = p.apply("ReadMyFile",
		// TextIO.read().from(options.getInputFile()));

		// log out the lines
		// lines.apply("Log inputs", // the transform name
		// ParDo.of(new DoFn<String,String>() { // a DoFn as an anonymous inner class
		// instance
		//
		// private static final long serialVersionUID = 1L;
		//
		// @ProcessElement
		// public void processElement(@Element String in, OutputReceiver<String> out) {
		// LOG.info("\n\n\n"+in+"\n\n\n");
		// }
		// }));

		// split the lines into an array
		// minor field is checked and added to valid records
		final PCollection<Person> split = lines.apply(new StringsToPeople());

		// log out the split lines
		// LOG.info("\n\nSPLIT LINES\n\n");
		// split.apply("Log splits", // the transform name
		// ParDo.of(new DoFn<Person,String[]>() { // a DoFn as an anonymous inner class
		// instance
		//
		// private static final long serialVersionUID = 1L;
		//
		// @ProcessElement
		// public void processElement(@Element Person in, OutputReceiver<String[]> out)
		// {
		// LOG.info(in.toNiceString());
		// }
		// }));

		// split the date into valid and invalid records
		// invalid records are defined by empty items
		final PCollectionList<Person> validityOfRecords = split.apply(Partition.of(2, new PartitionValidRecords()));

		final PCollection<Person> valid = validityOfRecords.get(0);
		final PCollection<Person> invalid = validityOfRecords.get(1);

		// log the invalid records
		LOG.info("\n\nINVALID LINES\n\n");
		invalid.apply("Log invalid", // the transform name
				ParDo.of(new outputPersonData()));

		// write out the invalid records
		invalid.apply("Output Invalid Records", ParDo.of(new toStringForOutput())).apply("Write PubSub Events",
				PubsubIO.writeStrings().to(options.getInvalidOutputTopic()));

		/* .apply(TextIO.write().to(options.getOutputDirectory() + "_invalid")); */

		// write out the valid records
		valid.apply("Output Invalid Records", ParDo.of(new toStringForOutput())).apply("Write to PubSub",
				PubsubIO.writeStrings().to(options.getValidOutputTopic()));

		/* .apply(TextIO.write().to(options.getOutputDirectory() + "_valid")); */

		p.run().waitUntilFinish();
	}

	public static void main(final String[] args) {
		BioStatsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BioStatsOptions.class);

		runBioStats(options);
	} // psvm
} // bioStatsPipe
