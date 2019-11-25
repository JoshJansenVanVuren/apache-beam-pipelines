package org.ambrite.josh;

import java.io.Serializable;
import java.util.Collections;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.fs.ResourceId;



/**
 * An example usage:
 * <pre>
 * mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_NAME} \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=${PROJECT_ID} \
 --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 --runner=DataflowRunner \
 --windowDuration=2m \
 --numShards=1 \
 --inputTopic=projects/${PROJECT_ID}/topics/windowed-files \
 --outputDirectory=gs://${PROJECT_ID}/temp/ \
 --outputFilenamePrefix=windowed-file \
 --outputFilenameSuffix=.txt"
 * </pre>
 */

public class bioStatsPipe {
	public enum ThreeState {
		TRUE,
		FALSE,
		UNSET
	};
	// declare the logger
	private static final Logger LOG = LoggerFactory.getLogger(bioStatsPipe.class);

	// **************************
	// ** define record object **
	// **************************
	static class Person extends Object implements Serializable {
		private static final long serialVersionUID = 1L;

		private final String name;
		private final String sex;
		private final int age;
		private final int weight;
		private final int height;
		private ThreeState minor;
 
		// constructor
		public Person(String name,String sex,int age, int weight, int height) {
			this.name = name;
			this.sex = sex;
			this.age = age;
			this.weight = weight;
			this.height = height;
			this.minor = ThreeState.UNSET;
		}


		// another alternative
		public Person(String name,String sex,int age, int weight, int height, ThreeState minor) {
			this.name = name;
			this.sex = sex;
			this.age = age;
			this.weight = weight;
			this.height = height;
			this.minor = minor;
		}
	
		// getter
		public String getName() {return name;}
		public String getSex() {return sex;}
		public int getAge() {return age;}
		public int getWeight() {return weight;}
		public int getHeight() {return height;}
		public ThreeState getIsMinor() {return minor;}

		@Override
		public boolean equals(final Object o) {	  
			// If the object is compared with itself then return true   
			if (o == this) { 
				return true; 
			} 
	  
			/* Check if o is an instance of Person or not 
			  "null instanceof [type]" also returns false */
			if (!(o instanceof Person)) { 
				return false; 
			} 
			  
			// typecast o to Person so that we can compare data members  
			final Person c = (Person) o; 
			  
			// Compare the data members and return accordingly  
			boolean members = true;

			if (!c.name.equals(this.name)) members = false;
			if (!c.sex.equals(this.sex)) members = false;
			if (!(c.age == this.age)) members = false;
			if (!(c.weight == this.weight)) members = false;
			if (!(c.height == this.height)) members = false;
			if (!(c.minor == this.minor)) members = false;

			return members;
		}

		@Override
		public String toString() {
			String output =
			this.name +", "
			+ this.sex + ", "
			+ this.age + ", "
			+ this.weight + ", "
			+ this.height;

			if (minor != ThreeState.UNSET) {
				if (minor == ThreeState.TRUE) {
					output = output + ", true";
				} else {
					output = output + ", false";
				}
			}

			return output;
		}

		public String toNiceString() {
			String output =
			"\nName: " + this.name +
			"\tSex: " + this.sex + 
			"\tAge: "+ this.age +
			"\tWeight: "+ this.weight + 
			"\tHeight: "+ this.height;

			if (minor != ThreeState.UNSET) {
				if (minor == ThreeState.TRUE) {
					output = output + "\tMinor: true";
				} else {
					output = output + "\tMinor: false\n";
				}
			}

			return output;
		}
	}

	// ****************************************************************
	// ** transform for conversion between String and Person objects **
	// *********************************************************s*******
	public static class StringsToPeople extends PTransform<PCollection<String>, PCollection<Person>> {
		private static final long serialVersionUID = 1L;

		@Override
		public PCollection<Person> expand(PCollection<String> lines) {
			// convert lines of text into people objects
			PCollection<Person> split = lines.apply( ParDo.of(new DoFn<String,Person>() { 
				private static final long serialVersionUID = 1L;
	
				@ProcessElement
				public void processElement(@Element String in, OutputReceiver<Person> out) {
					String[] outTemp = new String[5];
					int arrCounter = 0;

					int subStrStart = 0;
					for (int i = 0;i < in.length();i++) {
						// ignore whitespace before start
						if (i > 0 && in.charAt(i-1) == ' ' && in.charAt(i) != ' ') {
							subStrStart = i;
						}

						if (in.charAt(i) == ',') {
							outTemp[arrCounter] = in.substring(subStrStart, i);
							subStrStart = i + 1;
							arrCounter++;
						} // if
					} // for

					outTemp[arrCounter] = in.substring(subStrStart, in.length());

					ThreeState minor = ThreeState.UNSET;
					if (StringUtils.isNotBlank(outTemp[2])) {
						if (Integer.parseInt(outTemp[2]) < 18) {
							minor = ThreeState.TRUE;
						} else {
							minor = ThreeState.FALSE;
						} // else
					} // if

					LOG.info(outTemp.toString());


					Person personTemp = new Person(
						outTemp[0],
						outTemp[1],
						StringUtils.isNotBlank(outTemp[2]) ? Integer.parseInt(outTemp[2]) : 0,
						StringUtils.isNotBlank(outTemp[3]) ? Integer.parseInt(outTemp[3]) : 0,
						StringUtils.isNotBlank(outTemp[4]) ? Integer.parseInt(outTemp[4]) : 0,
						minor
					);

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
			if (person.getName().isEmpty()) flag = true;
			if (person.getSex().isEmpty()) flag = true;
			if (person.getAge() == 0) flag = true;
			if (person.getHeight() == 0) flag = true;
			if (person.getWeight() == 0) flag = true;

			// if any empty then add to list
			if (flag) {return 1;}
			else {return 0;}
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
	// ** DoFn to perform a toNiceString to prepare PColletion<Person> for logging **
	// ******************************************************************************
	static class outputPersonData extends DoFn<Person, Person> {					
		private static final long serialVersionUID = 1L;
	
		@ProcessElement
		public void processElement(@Element final Person in, final OutputReceiver<Person> out) {
			LOG.info(in.toNiceString());
		}
	}

	/** Converts strings into BigQuery rows. */
	static class StringToRowConverter extends DoFn<String, TableRow> {
		private static final long serialVersionUID = 1L;

		/** In this example, put the whole string into single BigQuery field. */
		@ProcessElement
		public void processElement(ProcessContext c) {
			c.output(new TableRow().set("string_field", c.element()));
		}

		static TableSchema getSchema() {
			return new TableSchema()
				.setFields(
					Collections.singletonList(
						new TableFieldSchema().setName("string_field").setType("STRING")));
		}
	}

	// *************************
	// ** gs pipeline options **
	// *************************
	public interface BioStatsOptions extends ExampleOptions, StreamingOptions, BigQueryTableOptions {
		@Description("The Cloud Pub/Sub topic to read from.")
		@Required
		ValueProvider<String> getInputTopic();
		void setInputTopic(ValueProvider<String> value);

		@Description(
        "The Cloud Pub/Sub topic to publish to. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
		@Validation.Required
		ValueProvider<String> getValidOutputTopic();
		void setValidOutputTopic(ValueProvider<String> outputTopic);

		@Description(
        "The Cloud Pub/Sub topic to publish to. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
		@Validation.Required
		ValueProvider<String> getInvalidOutputTopic();
		void setInvalidOutputTopic(ValueProvider<String> outputTopic);

	}	

	static void runBioStats(BioStatsOptions options) {
		// create pipleline
		Pipeline p = Pipeline.create(options);

		options.setStreaming(true);

		String tableSpec =
        new StringBuilder()
            .append(options.getProject())
            .append(":")
            .append(options.getBigQueryDataset())
            .append(".")
            .append(options.getBigQueryTable())
            .toString();
		
		
    	PCollection<String> lines = p
        .apply("Read PubSub Events", PubsubIO.readStrings().fromTopic(options.getInputTopic()));
        /*.apply(
            options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))));
		
        // Apply windowed file writes. Use a NestedValueProvider because the filename
        // policy requires a resourceId generated from the input value at runtime.
        .apply(
            "Write File(s)",
            TextIO.write()
                .withWindowedWrites()
                .withNumShards(options.getNumShards())
                .to(
                    new WindowedFilenamePolicy(
                        options.getOutputDirectory(),
                        options.getOutputFilenamePrefix(),
                        options.getOutputShardTemplate(),
                        options.getOutputFilenameSuffix()))
                .withTempDirectory(NestedValueProvider.of(
                    options.getOutputDirectory(),
                    (SerializableFunction<String, ResourceId>) input ->
                        FileBasedSink.convertToFileResourceIfPossible(input))));*/

		
		// PCollection<String> lines = p.apply("ReadMyFile", TextIO.read().from(options.getInputFile()));

		// log out the lines
		//lines.apply("Log inputs",                     // the transform name
		//	ParDo.of(new DoFn<String,String>() {    // a DoFn as an anonymous inner class instance
		//			
		//		private static final long serialVersionUID = 1L;
		//
		//	@ProcessElement
		//	public void processElement(@Element String in, OutputReceiver<String> out) {
		//		LOG.info("\n\n\n"+in+"\n\n\n");
		//	}
		//}));

		// split the lines into an array
		// minor field is checked and added to valid records
		final PCollection<Person> split = lines.apply(new StringsToPeople());

		// log out the split lines
		//LOG.info("\n\nSPLIT LINES\n\n");
		//split.apply("Log splits",                     // the transform name
		//	ParDo.of(new DoFn<Person,String[]>() {    // a DoFn as an anonymous inner class instance
		//			
		//		private static final long serialVersionUID = 1L;
		//
		//	@ProcessElement
		//	public void processElement(@Element Person in, OutputReceiver<String[]> out) {
		//		LOG.info(in.toNiceString());
		//	}
		//}));

		// split the date into valid and invalid records
		// invalid records are defined by empty items
		final PCollectionList<Person> validityOfRecords =
		split.apply(Partition.of(2, new PartitionValidRecords()));

		final PCollection<Person> valid = validityOfRecords.get(0);
		final PCollection<Person> invalid = validityOfRecords.get(1);

		// log the invalid records
		LOG.info("\n\nINVALID LINES\n\n");
		invalid.apply(
			"Log invalid",                     // the transform name
			ParDo.of(new outputPersonData()));

		// write out the invalid records
		invalid.apply(
			"Output Invalid Records",
			ParDo.of(new toStringForOutput()))
			.apply("Write PubSub Events", PubsubIO.writeMessages().to(options.getInvalidOutputTopic()));
		
					
			/*.apply(TextIO.write().to(options.getOutputDirectory() + "_invalid"));*/

		// write out the valid records
		valid.apply("Output Invalid Records",
			ParDo.of(new toStringForOutput()))
			.apply("Write PubSub Events", PubsubIO.writeMessages().to(options.getValidOutputTopic()));
			
			/*.apply(TextIO.write().to(options.getOutputDirectory() + "_valid"));*/

		p.run().waitUntilFinish();
	}

	public static void main(final String[] args) {
		BioStatsOptions options =
		PipelineOptionsFactory.fromArgs(args).withValidation().as(BioStatsOptions.class);
		
		runBioStats(options);
	} //psvm
} // bioStatsPipe
