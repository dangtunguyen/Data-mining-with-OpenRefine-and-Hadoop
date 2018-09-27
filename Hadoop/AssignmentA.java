{\rtf1\ansi\ansicpg1252\cocoartf1504\cocoasubrtf600
{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;\red255\green255\blue255;}
{\*\expandedcolortbl;\csgray\c100000;\csgray\c0;\csgray\c100000;}
\margl1440\margr1440\vieww12600\viewh10200\viewkind0
\pard\tx560\tx1120\tx1680\tx2240\tx2800\tx3360\tx3920\tx4480\tx5040\tx5600\tx6160\tx6720\pardirnatural\partightenfactor0

\f0\fs28 \cf2 \cb3 \CocoaLigature0 import java.io.BufferedReader;\
import java.io.FileReader;\
import java.io.IOException;\
import java.net.URI;\
import java.util.ArrayList;\
import java.util.HashSet;\
import java.util.List;\
import java.util.Set;\
import java.util.StringTokenizer;\
\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.fs.Path;\
import org.apache.hadoop.io.IntWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Job;\
import org.apache.hadoop.mapreduce.Mapper;\
import org.apache.hadoop.mapreduce.Reducer;\
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;\
import org.apache.hadoop.mapreduce.Counter;\
import org.apache.hadoop.util.GenericOptionsParser;\
import org.apache.hadoop.util.StringUtils;\
\
public class WordCount \{\
\
  public static class TokenizerMapper\
       extends Mapper<Object, Text, Text, IntWritable>\{\
\
    static enum CountersEnum \{ INPUT_WORDS \}\
\
    private final static IntWritable one = new IntWritable(1);\
    private Text city = new Text();\
\
    private boolean caseSensitive;\
    private Set<String> patternsToSkip = new HashSet<String>();\
\
    private Configuration conf;\
    private BufferedReader fis;\
\
    @Override\
    public void setup(Context context) throws IOException,\
        InterruptedException \{\
      conf = context.getConfiguration();\
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);\
      if (conf.getBoolean("wordcount.skip.patterns", true)) \{\
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();\
        if (patternsURIs != null) \{\
		for (URI patternsURI : patternsURIs) \{\
          		Path patternsPath = new Path(patternsURI.getPath());\
          		String patternsFileName = patternsPath.getName().toString();\
          		parseSkipFile(patternsFileName);\
        	\}\
	\}\
	else \{\
		System.err.println("patternsURIs is NULL\\n");\
	\}\
      \}\
    \}\
\
    private void parseSkipFile(String fileName) \{\
      try \{\
        fis = new BufferedReader(new FileReader(fileName));\
        String pattern = null;\
        while ((pattern = fis.readLine()) != null) \{\
          patternsToSkip.add(pattern);\
        \}\
      \} catch (IOException ioe) \{\
        System.err.println("Caught exception while parsing the cached file '"\
            + StringUtils.stringifyException(ioe));\
      \}\
    \}\
\
    @Override\
    public void map(Object key, Text value, Context context\
                    ) throws IOException, InterruptedException \{\
      String line = (caseSensitive) ?\
          value.toString() : value.toString().toLowerCase();\
      for (String pattern : patternsToSkip) \{\
        line = line.replaceAll(pattern, "");\
      \}\
      /* Format of the input data is
       * "Conference Acronym" \t "Year" \t "Conference Name" \t "Location" */
      /* Split by tab */\
      StringTokenizer itr = new StringTokenizer(line,"\\t");\
      /* Skip the first three columns */\
      for (int i = 0; i < 3; i++) \{\
      	if (itr.hasMoreTokens()) \{\
		itr.nextToken();\
	\} else \{\
		System.err.println("Bad line: " + line + "\\n");\
		break;\
	\}\
      \}	\
      /* We need to process the "City" column */\
      if (itr.hasMoreTokens()) \{\
        city.set(itr.nextToken());\
        /* Use "city" column as key and number of occurrences as value */
        context.write(city, one);\
        Counter counter = context.getCounter(CountersEnum.class.getName(),\
            CountersEnum.INPUT_WORDS.toString());\
        counter.increment(1);\
      \}\
    \}\
  \}\
\
  public static class IntSumReducer\
       extends Reducer<Text,IntWritable,Text,IntWritable> \{\
    private IntWritable result = new IntWritable();\
\
    public void reduce(Text key, Iterable<IntWritable> values,\
                       Context context\
                       ) throws IOException, InterruptedException \{\
      int sum = 0;\
      for (IntWritable val : values) \{\
    	/* Just sum up number of conferences (value) */
        sum += val.get();\
      \}\
      result.set(sum);\
      context.write(key, result);\
    \}\
  \}\
\
  public static void main(String[] args) throws Exception \{\
    Configuration conf = new Configuration();\
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);\
    String[] remainingArgs = optionParser.getRemainingArgs();\
    if (!(remainingArgs.length != 2 ||  remainingArgs.length != 4)) \{\
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");\
      System.exit(2);\
    \}\
    Job job = Job.getInstance(conf, "word count");\
    job.setJarByClass(WordCount.class);\
    job.setMapperClass(TokenizerMapper.class);\
    job.setCombinerClass(IntSumReducer.class);\
    job.setReducerClass(IntSumReducer.class);\
    job.setOutputKeyClass(Text.class);\
    job.setOutputValueClass(IntWritable.class);\
\
    List<String> otherArgs = new ArrayList<String>();\
    for (int i=0; i < remainingArgs.length; ++i) \{\
      if ("-skip".equals(remainingArgs[i])) \{\
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());\
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);\
      \} else \{\
        otherArgs.add(remainingArgs[i]);\
      \}\
    \}\
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));\
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));\
\
    System.exit(job.waitForCompletion(true) ? 0 : 1);\
  \}\
\}}