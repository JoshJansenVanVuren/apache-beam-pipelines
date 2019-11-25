package org.ambrite.josh;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WordCountJosh {

	private static final Logger LOG = LoggerFactory.getLogger(WordCountJosh.class);

	static class ExtractWordsFn extends DoFn<String, String> {	
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {
		}
	}

	public static class CountWords extends PTransform<PCollection<String>,
		PCollection<KV<String, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
	public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

		// Convert lines of text into individual words.
		PCollection<String> words = lines.apply(
			ParDo.of(new ExtractWordsFn()));

		// Count the number of times each word occurs.
		PCollection<KV<String, Long>> wordCounts =
			words.apply(Count.<String>perElement());

		return wordCounts;
	}
	}
	  
	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline p = Pipeline.create(options);
	
		// Split lines
		PCollection<String> lines = p.apply("ReadMyFile", TextIO.read().from("skrip_input.txt"));

		// lines.apply("Log lines",                     // the transform name
		// ParDo.of(new DoFn<String, String>() {    // a DoFn as an anonymous inner class instance
					
		// 			private static final long serialVersionUID = 1L;

		// 			@ProcessElement
		// 	public void processElement(@Element String in, OutputReceiver<String> out) {
		// 		LOG.info("\n\n\n"+in+"\n\n\n");
		// 	}
		// })).apply(TextIO.write().to("max"));
		
		// Split into words
		PCollection<String> wordsWithEmpties = lines.apply(
			FlatMapElements.into(TypeDescriptors.strings())
    		.via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))));
		
		//Remove empty words
  		PCollection<String> words = wordsWithEmpties.apply(Filter.by((String word) -> !word.isEmpty()));
		
		//Tallies occurances of words
		  PCollection<KV<String, Long>> counts = words.apply(Count.perElement());
		  
		  counts.apply("Log counts",                     // the transform name
		ParDo.of(new DoFn<KV<String, Long>, String>() {    // a DoFn as an anonymous inner class instance
					
					private static final long serialVersionUID = 1L;

					@ProcessElement
			public void processElement(@Element KV<String, Long> in, OutputReceiver<String> out) {
				LOG.info("\n\n\n"+in+"\n\n\n");
			}
		})).apply(TextIO.write().to("max"));

		//Just the values
		PCollection<Long> countsVals = counts.apply(Values.<Long>create());
		
		PCollection<Long> max = countsVals.apply(Max.longsGlobally());

		max.apply("ConverToString",                     // the transform name
		ParDo.of(new DoFn<Long, String>() {    // a DoFn as an anonymous inner class instance
					/**
					 *
					 */
					private static final long serialVersionUID = 1L;

					@ProcessElement
			public void processElement(@Element Long in, OutputReceiver<String> out) {
			  out.output(in.toString());
			}
		})).apply(TextIO.write().to("max"));
		
		//Convert to strings for export to .txt file
		counts.apply(
			MapElements.into(TypeDescriptors.strings())
				.via(
  				(KV<String, Long> wordCount) ->
       		wordCount.getKey() + ": " + wordCount.getValue()))
        
		.apply(TextIO.write().to("wordcounts"));

		p.run().waitUntilFinish();
  }
}
