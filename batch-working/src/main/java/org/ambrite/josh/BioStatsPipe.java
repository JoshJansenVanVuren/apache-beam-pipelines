package org.ambrite.josh;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import org.ambrite.josh.Constants.ThreeState;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.commons.lang3.StringUtils;

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
						if ((i > 0) && (in.charAt(i - 1) == ' ') && (in.charAt(i) != ' ')) {
							subStrStart = i;
						}

						if (in.charAt(i) == Constants.DELIMITER) {
							members.add(in.substring(subStrStart, i));
							subStrStart = i + 1;
						} // if
					} // for

					// split the last member in the list
					members.add(in.substring(subStrStart, in.length()));

					// verify the "is minor" field
					ThreeState minor = ThreeState.UNSET;
					if (StringUtils.isNotBlank(members.get(Constants.AGE_INDEX))) {
						if (Integer.parseInt(members.get(Constants.AGE_INDEX)) < Constants.AGE_OF_MINOR) {
							minor = ThreeState.TRUE;
						} else {
							minor = ThreeState.FALSE;
						} // else
					} // if

					if (Constants.DEBUGGING_MODE)
						LOG.info(members.toString());

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

	// *********************************************************************
	// ** DoFn to perform a toString to prepare PColletion for txt output **
	// *********************************************************************
	static class toStringForOutput extends DoFn<Person, String> {
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(@Element final Person in, final OutputReceiver<String> out) {
			out.output(in.toString());
		}
	}

	// **********************************************************************
	// ** DoFn to perform a toNiceString to prepare PColletion for logging **
	// **********************************************************************
	static class outputPersonData extends DoFn<Person, Person> {
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(@Element final Person in, final OutputReceiver<Person> out) {
			LOG.info(in.toNiceString());
		}
	}

	// ***************************************************************
	// ** DoFn to perform the tagging for invalid and valid records **
	// ***************************************************************
	static class tagValidAndInvalidRecords extends DoFn<String, String> {
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext in) {
			ArrayList<String> members = new ArrayList<String>();

			// split the string into an array list
			int subStrStart = 0;
			for (int i = 0; i < in.element().length(); i++) {
				// ignore whitespace before start
				if ((i > 0) && (in.element().charAt(i - 1) == ' ') && (in.element().charAt(i) != ' ')) {
					subStrStart = i;
				}

				if (in.element().charAt(i) == Constants.DELIMITER) {
					members.add(in.element().substring(subStrStart, i));
					subStrStart = i + 1;
				} // if
			} // for

			// split the last member in the list
			members.add(in.element().substring(subStrStart, in.element().length()));

			if (Utils.ensureInputStringValid(members)) {
				in.output(Constants.validRecordTag, in.element());
			} else {
				in.output(Constants.invalidRecordTag, in.element());
			}
		}
	}

	// *************************
	// ** gs pipeline options **
	// *************************
	public interface BioStatsOptions extends PipelineOptions {

		/**
		 * Set this option to specify where to read the input from.
		 */
		@Description("Path of the file to read from")
		@Default.String("biostats.csv")
		String getInputFile();

		void setInputFile(String value);

		/**
		 * Set this option to specify where to write the output.
		 */
		@Description("Path of the file to write to")
		@Default.String("output")
		String getOutput();

		void setOutput(String value);
	}

	static void runBioStats(BioStatsOptions options) {
		// create pipleline
		Pipeline p = Pipeline.create(options);
		PCollection<String> lines = p.apply("ReadMyFile", TextIO.read().from(options.getInputFile()));

		// split the data into valid and invalid records
		// invalid records are defined by empty items
		PCollectionTuple mixedCollection = lines.apply(ParDo.of(new tagValidAndInvalidRecords())
				.withOutputTags(Constants.validRecordTag, TupleTagList.of(Constants.invalidRecordTag)));

		// Get subset of the output with tag validRecordTag.
		final PCollection<String> valid = mixedCollection.get(Constants.validRecordTag);

		// Get subset of the output with tag invalidRecordTag.
		final PCollection<String> invalid = mixedCollection.get(Constants.invalidRecordTag);

		// convert string to person
		// minor field is checked and added to valid records
		final PCollection<Person> valid_person = valid.apply(new StringsToPeople());

		// write out the invalid records
		invalid.apply(TextIO.write().to(options.getOutput() + "_invalid"));

		// write out the valid records
		valid_person.apply("Output Valid Fields", ParDo.of(new toStringForOutput()))
				.apply(TextIO.write().to(options.getOutput() + "_valid"));

		p.run().waitUntilFinish();
	}

	public static void main(final String[] args) {
		BioStatsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BioStatsOptions.class);

		runBioStats(options);
	} // psvm
} // bioStatsPipe
