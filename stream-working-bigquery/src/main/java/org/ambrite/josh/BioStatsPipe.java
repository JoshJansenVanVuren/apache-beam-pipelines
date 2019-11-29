package org.ambrite.josh;

import org.ambrite.josh.Constants;
import org.ambrite.josh.Constants.ThreeState;
import java.util.ArrayList;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avro.shaded.com.google.common.collect.ImmutableList;

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
						if (Integer.parseInt(members.get(Constants.AGE_INDEX)) < 18) {
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
	public interface BioStatsOptions extends ExampleOptions, StreamingOptions {
		@Description("The Cloud Pub/Sub topic to read from.")
		@Required
		ValueProvider<String> getInputTopic();

		void setInputTopic(ValueProvider<String> value);

		@Description("The Big Query Table to publish to. " + "The table should be in the format of "
				+ "{PROJECT}:{DATA-SET}.{TABLE-NAME}")
		@Validation.Required
		ValueProvider<String> getBigQueryTable();

		void setBigQueryTable(ValueProvider<String> bigQueryTable);

	}

	static void runBioStats(BioStatsOptions options) {
		// create pipleline
		Pipeline p = Pipeline.create(options);

		options.setStreaming(true);

		PCollection<String> lines = p.apply("Read PubSub Events",
				PubsubIO.readStrings().fromTopic(options.getInputTopic()));

		// split the data into valid and invalid records
		// invalid records are defined by empty items
		PCollectionTuple mixedCollection = lines.apply(ParDo.of(new tagValidAndInvalidRecords())
				.withOutputTags(Constants.validRecordTag, TupleTagList.of(Constants.invalidRecordTag)));

		// Get subset of the output with tag validRecordTag.
		final PCollection<String> valid = mixedCollection.get(Constants.validRecordTag);

		// Get subset of the output with tag invalidRecordTag.
		final PCollection<String> invalid = mixedCollection.get(Constants.invalidRecordTag);

		// split the lines into an array
		// minor field is checked and added to valid records
		final PCollection<Person> valid_person = valid.apply(new StringsToPeople());

		valid_person.apply(BigQueryIO.<Person>write().to(options.getBigQueryTable())
				.withSchema(new TableSchema()
						.setFields(ImmutableList.of(new TableFieldSchema().setName("name").setType("STRING"),
								new TableFieldSchema().setName("sex").setType("STRING"),
								new TableFieldSchema().setName("age").setType("INT"),
								new TableFieldSchema().setName("weight").setType("INT"),
								new TableFieldSchema().setName("weight").setType("INT"))))
				.withFormatFunction((Person elem) -> new TableRow().set("name", elem.getName())
						.set("sex", elem.getSex()).set("age", elem.getAge()).set("weight", elem.getWeight())
						.set("height", elem.getHeight()))
				.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(WriteDisposition.WRITE_APPEND));

		p.run().waitUntilFinish();
	}

	public static void main(final String[] args) {
		BioStatsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BioStatsOptions.class);

		runBioStats(options);
	} // psvm
} // bioStatsPipe
