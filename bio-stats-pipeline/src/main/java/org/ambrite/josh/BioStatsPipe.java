package org.ambrite.josh;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.ambrite.josh.Constants.ThreeState;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.StringUtils;

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

	// **********************************
	// ** local batch pipeline options **
	// **********************************
	public interface BioStatsOptionsBatchLocal extends PipelineOptions {

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

	// ******************************************
	// ** streaming pubsub gs pipeline options **
	// ******************************************
	public interface BioStatsOptionsPubSub extends StreamingOptions {
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

		@Description("Whether to keep jobs running after local process exit")
		@Default.Boolean(false)
		boolean getKeepJobsRunning();
	  
		void setKeepJobsRunning(boolean keepJobsRunning);
	  
		@Description("Number of workers to use when executing the injector pipeline")
		@Default.Integer(1)
		int getInjectorNumWorkers();
	  
		void setInjectorNumWorkers(int numWorkers);
	}
	
	// *****************************************
	// ** streaming bigquery pipeline options **
	// *****************************************
	public interface BioStatsOptionsBigQuery extends StreamingOptions {
		@Description("The Cloud Pub/Sub topic to read from.")
		@Required
		ValueProvider<String> getInputTopic();

		void setInputTopic(ValueProvider<String> value);

		@Description("The Big Query Table to publish to. " + "The table should be in the format of "
				+ "{PROJECT}:{DATA-SET}.{TABLE-NAME}")
		@Validation.Required
		ValueProvider<String> getBigQueryTable();

		void setBigQueryTable(ValueProvider<String> bigQueryTable);

		@Description("Whether to keep jobs running after local process exit")
		@Default.Boolean(false)
		boolean getKeepJobsRunning();
	  
		void setKeepJobsRunning(boolean keepJobsRunning);
	  
		@Description("Number of workers to use when executing the injector pipeline")
		@Default.Integer(1)
		int getInjectorNumWorkers();
	  
		void setInjectorNumWorkers(int numWorkers);
	}

	// *******************************************
	// ** local runner of the biostats pipeline **
	// *******************************************
	static void runBioStatsLocal(BioStatsOptionsBatchLocal options) {
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

	// ****************************************************
	// ** dataflow runner (input from pubsub, to pubsub) **
	// **           of the biostats pipeline             **
	// ****************************************************
	static void runBioStatsPubSub(BioStatsOptionsPubSub options) {
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
		
		// convert string to person
		// minor field is checked and added to valid records
		final PCollection<Person> valid_person = valid.apply(new StringsToPeople());

		// write out the invalid records
		invalid.apply("Write PubSub Events",
				PubsubIO.writeStrings().to(options.getInvalidOutputTopic()));

		// write out the valid records
		valid_person.apply("Output Valid Records", ParDo.of(new toStringForOutput())).apply("Write to PubSub",
				PubsubIO.writeStrings().to(options.getValidOutputTopic()));

		p.run().waitUntilFinish();
	}

	// *******************************************************
	// ** dataflow runner (input from pubsub, to big query) **
	// **             of the biostats pipeline              **
	// *******************************************************
	static void runBioStatsBigQuery(BioStatsOptionsBigQuery options) {
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
		// final PCollection<String> invalid = mixedCollection.get(Constants.invalidRecordTag);

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
		switch(Constants.state) {
			case BATCH_LOCAL:
				BioStatsOptionsBatchLocal optionsBatch = PipelineOptionsFactory.fromArgs(args).withValidation().as(BioStatsOptionsBatchLocal.class);
				runBioStatsLocal(optionsBatch);
			break;
			case STREAMING_PUBSUB:
				BioStatsOptionsPubSub optionsPubSub = PipelineOptionsFactory.fromArgs(args).withValidation().as(BioStatsOptionsPubSub.class);
				runBioStatsPubSub(optionsPubSub);
			break;
			case STREAMING_BIGQUERY:
				BioStatsOptionsBigQuery optionsBigQuery = PipelineOptionsFactory.fromArgs(args).withValidation().as(BioStatsOptionsBigQuery.class);
				runBioStatsBigQuery(optionsBigQuery);
			break;
		}
	} // psvm
} // bioStatsPipe
