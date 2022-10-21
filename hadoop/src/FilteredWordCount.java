import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilteredWordCount {

	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private ArrayList<String> nonApplicableStopWords = GetStopWords();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String filteredString = FilterString(value);
			StringTokenizer itr = new StringTokenizer(filteredString);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken().toLowerCase();
				if (!nonApplicableStopWords.contains(token)) {
					word.set(token);
					context.write(word, one);
				}
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class Top200Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Map<String, Integer> countMap = new HashMap<String, Integer>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			countMap.put(key.toString(), sum);
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			List<Entry<String, Integer>> sorted = countMap.entrySet().stream()
			        .sorted(Comparator
			            .comparing(Map.Entry<String, Integer>::getValue).reversed()
			            .thenComparing(Map.Entry<String, Integer>::getKey))
			        .collect(Collectors.toList());

			for (Map.Entry<String, Integer> entry : sorted.subList(0, 200)) {
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}
	
	public static class AverageLengthMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();
		private ArrayList<String> nonApplicableStopWords = GetStopWords();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String filteredString = FilterString(value);
			StringTokenizer itr = new StringTokenizer(filteredString);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken().toLowerCase();
				if (!nonApplicableStopWords.contains(token)) {
					word.set(String.valueOf(token.charAt(0)));
					context.write(word, new IntWritable(token.length()));
				}
			}
		}
	}

	public static class AverageLengthReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int totalWordLengthSum = 0;
			int wordCount = 0;
			for (IntWritable val : values) {
				totalWordLengthSum += val.get();
				wordCount++;
			}
			
			context.write(new Text(key), new DoubleWritable((double) totalWordLengthSum / wordCount));
		}
	}
	
	private static String FilterString(Text value) {
		return value.toString().replaceAll("[^a-zA-Z0-9\\s]", "");
	}
	
	private static ArrayList<String> GetStopWords() {
		return new ArrayList<String>(Arrays.asList("i", "me", "my",
				"myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he",
				"him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them",
				"their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am",
				"is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did",
				"doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at",
				"by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after",
				"above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
				"further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
				"few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
				"too", "very", "s", "t", "can", "will", "just", "don", "should", "now"));
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job wordCountJob = new Job(conf, "word_count");
		wordCountJob.setJarByClass(FilteredWordCount.class);
		wordCountJob.setMapperClass(WordCountMapper.class);
		wordCountJob.setReducerClass(WordCountReducer.class);
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1] + "wc"));
		wordCountJob.waitForCompletion(true);
		if(!wordCountJob.isSuccessful()) {
			System.exit(0);
		}
		
		Job top200Job = new Job(conf, "top 200 repeated words");
		top200Job.setJarByClass(FilteredWordCount.class);
		top200Job.setMapperClass(WordCountMapper.class);
		top200Job.setReducerClass(Top200Reducer.class);
		top200Job.setOutputKeyClass(Text.class);
		top200Job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(top200Job, new Path(args[0]));
		FileOutputFormat.setOutputPath(top200Job, new Path(args[1] + "top200"));
		top200Job.waitForCompletion(true);
		if(!top200Job.isSuccessful()) {
			System.exit(0);
		}
		
		Job averageLengthJob = new Job(conf, "Average length of words");
		averageLengthJob.setJarByClass(FilteredWordCount.class);
		averageLengthJob.setMapperClass(AverageLengthMapper.class);
		averageLengthJob.setReducerClass(AverageLengthReducer.class);
		averageLengthJob.setOutputKeyClass(Text.class);
		averageLengthJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(averageLengthJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(averageLengthJob, new Path(args[1] + "average"));
		System.exit(averageLengthJob.waitForCompletion(true) ? 0 : 1);
	}
}
