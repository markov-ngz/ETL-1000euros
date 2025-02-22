package com.markovngz.hdfs ;  

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class HDFSHandler {
    private  FileSystem fileSystem;
    private final Configuration conf;
    private final String hdfsUri;

    /**
     * Constructor initializes HDFS connection
     * @param hdfsUri The HDFS cluster URI (e.g., "hdfs://localhost:9000")
     * @throws IOException If connection fails
     */
    public HDFSHandler(String hdfsUri)  {
        this.hdfsUri = hdfsUri;
        this.conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
    }

    public void connect()throws IOException {
        this.fileSystem = FileSystem.get(conf);
    }

    /**
     * Write content to a file in HDFS
     * @param content Content to write
     * @param hdfsPath Destination path in HDFS
     * @throws IOException If write operation fails
     */
    public void writeFile(String content, String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        FSDataOutputStream outputStream = fileSystem.create(path, true);
        outputStream.writeBytes(content);
        outputStream.close();
        System.out.println("File written successfully to: " + hdfsPath);
    }

    public void writeBytes(byte[] buffer, String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        FSDataOutputStream outputStream = fileSystem.create(path, true);
        outputStream.write(buffer);
        outputStream.close();
        System.out.println("File written successfully to: " + hdfsPath);
    }

    /**
     * Read content from a file in HDFS
     * @param hdfsPath Path of the file to read
     * @return Content of the file as String
     * @throws IOException If read operation fails
     */
    public String readFile(String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        StringBuilder content = new StringBuilder();
 
        FSDataInputStream inputStream = fileSystem.open(path); 

        java.util.Scanner s = new java.util.Scanner(inputStream).useDelimiter("\\A");
        content.append(s.hasNext() ? s.next() : "");
        s.close();
        
        
        return content.toString();
    }

    /**
     * Create a directory in HDFS
     * @param dirPath Directory path to create
     * @throws IOException If directory creation fails
     */
    public void createDirectory(String dirPath) throws IOException {
        Path path = new Path(dirPath);
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
            System.out.println("Directory created successfully: " + dirPath);
        } else {
            System.out.println("Directory already exists: " + dirPath);
        }
    }

    /**
     * Delete a file or directory from HDFS
     * @param path Path to delete
     * @param recursive If true, recursively delete directories
     * @throws IOException If deletion fails
     */
    public void delete(String path, boolean recursive) throws IOException {
        Path hdfsPath = new Path(path);
        if (fileSystem.exists(hdfsPath)) {
            fileSystem.delete(hdfsPath, recursive);
            System.out.println("Deleted successfully: " + path);
        } else {
            System.out.println("Path does not exist: " + path);
        }
    }

    /**
     * List contents of a directory
     * @param dirPath Directory path to list
     * @return List of file/directory names
     * @throws IOException If listing fails
     */
    public List<String> listDirectory(String dirPath) throws IOException {
        List<String> fileList = new ArrayList<>();
        Path path = new Path(dirPath);
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        
        for (FileStatus status : fileStatuses) {
            fileList.add(status.getPath().getName());
        }
        return fileList;
    }

    /**
     * Mapper class for word count example
     */
    public static class WordCountMapper 
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase());
                context.write(word, one);
            }
        }
    }

    /**
     * Reducer class for word count example
     */
    public static class WordCountReducer 
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

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

    /**
     * Run a word count MapReduce job
     * @param inputPath Input path in HDFS
     * @param outputPath Output path in HDFS
     * @throws Exception If job fails
     */
    public void runWordCountJob(String inputPath, String outputPath) throws Exception {
        // Delete output path if it exists
        Path outPath = new Path(outputPath);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        // Create and configure MapReduce job
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(HDFSHandler.class);
        
        // Set Mapper and Reducer classes
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        
        // Set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        // Wait for job completion
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("MapReduce job completed successfully");
        } else {
            throw new Exception("MapReduce job failed");
        }
    }

    /**
     * Close HDFS connection
     * @throws IOException If closing fails
     */
    public void close() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }
}