import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapred.Counters;

public class ParallelDijkstra {
    public static enum NodeCounter{COUNT};
    public static class DijkstraMapper extends Mapper<LongWritable, Text, IntWritable, PDNodeWritable> {

        
        int src_node;
        String first_itr;

        public void setup(Context context){
            Configuration conf = context.getConfiguration(); //use this to pass parameter         
            src_node = Integer.parseInt(conf.get("src"));
            first_itr = conf.get("first_itr");
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            PDNodeWritable currentNode = new PDNodeWritable();
            //start_node max_num -1 countneighbor neighbor weight neighbor weight
            StringTokenizer itr = new StringTokenizer(value.toString()); //fromString
            itr.nextToken(); //node ID
            
            
                currentNode.setNode(Integer.parseInt(itr.nextToken()));
                int distance = Integer.parseInt(itr.nextToken());
                if(currentNode.getNode() == src_node) distance = 0;
                currentNode.setDistance(distance);
                currentNode.setPrevNode(Integer.parseInt(itr.nextToken()));
                int size = Integer.parseInt(itr.nextToken());
            
                for (int i = 0; i < size ; i++){
                    int neighbornode = Integer.parseInt(itr.nextToken());
                    int weight = Integer.parseInt(itr.nextToken());
                    currentNode.addNeighbor(neighbornode, weight);
                    int updated_distance;
                    updated_distance = distance + weight;
                    PDNodeWritable tmp =  new PDNodeWritable(-2, updated_distance, currentNode.getNode());
                    context.write(new IntWritable(neighbornode), tmp);
                
            }
                context.write(new IntWritable(currentNode.getNode()), currentNode);
            
        }
    }

    public static class DijkstraReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {

        public static int MAX_INT = 1000000000;
        
        int src_node;
        
        public void setup(Context context){
            Configuration conf = context.getConfiguration(); 
            src_node = Integer.parseInt(conf.get("src"));
        }
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
                throws IOException, InterruptedException {
            int distance = MAX_INT;
            int prevnode = -1; 
            PDNodeWritable node = new PDNodeWritable();

            for (PDNodeWritable p : values) {
                if(key.get() == p.getNode()){ //if d is the passed on node struct
                    node = new PDNodeWritable(p); //deep copy
                }
                if(p.getDistance() < distance){
                    distance = p.getDistance();
                    prevnode = p.getPrevNode();
                }
            }

            
            int prevDist = node.getDistance();
            if(prevDist != distance){
                context.getCounter(NodeCounter.COUNT).increment(1);
            }
            if(distance < MAX_INT) node.setDistance(distance);
            if(prevnode  != -1){
                node.setPrevNode(prevnode);
                //System.out.println(prevnode);
            } 
            if(node.getNode() == -2) node.setNode(key.get());
            
            context.write(key, new Text(node.toString()));
            
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
		conf1.set("src", args[2]);
        conf1.set("iterations", args[3]);
		Job job1 = Job.getInstance(conf1,"preprocess");
		job1.setJarByClass(ParallelDijkstra.class);
		job1.setJar("paralleldijkstra.jar");
        job1.setMapperClass(PDPreProcess.PreprocessMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(PDPreProcess.PreprocessReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class); 
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
		Path output = new Path(args[1]);
        String tmpPath = output.getParent().toString() + "/tmp";
		Path outputJob1 = new Path(tmpPath + "/tmp0");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, outputJob1);
		FileSystem fs = FileSystem.get(conf1);
		if (fs.exists(outputJob1)) {
			System.out.println("deleted folder: " + "/tmp0");
			fs.delete(outputJob1, true);
		}
        //System.exit(job1.waitForCompletion(true) ? 0 : 1);
        int i = 0;
        
		if(job1.waitForCompletion(true)) {
			System.out.println("job1 completed");
            
			i = 0;
			int iteration = Integer.parseInt(args[3]);
			long flag = 1;
			while(true){
				if(iteration == 0){
					if(flag == 0) {
						System.out.println("job2 completed");
						break;
					}
				}else{
					if(flag == 0 || i == iteration){
						System.out.println("job2 completed");
						break;
					} 
				}
				
				Job job2 = Job.getInstance(conf1, "Parallel Dijkstra");
				job2.setJarByClass(ParallelDijkstra.class);
				job2.setJar("paralleldijkstra.jar");
                
				job2.setMapperClass(ParallelDijkstra.DijkstraMapper.class);
				job2.setReducerClass(ParallelDijkstra.DijkstraReducer.class);
                //System.out.println("111");
				job2.setMapOutputKeyClass(IntWritable.class);
				job2.setMapOutputValueClass(PDNodeWritable.class); 
                //System.out.println("222");
				job2.setOutputKeyClass(IntWritable.class);
                //System.out.println("333");
				job2.setOutputValueClass(Text.class);
                //System.out.println("444");
				Path outputJob2 = new Path(tmpPath + "/tmp" + Integer.toString(i + 1));

				FileInputFormat.addInputPath(job2, new Path(tmpPath + "/tmp" + Integer.toString(i)));
				FileOutputFormat.setOutputPath(job2, outputJob2);
				fs = FileSystem.get(conf1);
				if (fs.exists(outputJob2)) {
					System.out.println("deleted folder: /tmp" + Integer.toString(i + 1));
					fs.delete(outputJob2, true);
				}
				if(job2.waitForCompletion(true)) {;
					System.out.println("iteration: " + Integer.toString(i + 1));
					flag = job2.getCounters().findCounter(ParallelDijkstra.NodeCounter.COUNT).getValue();
					System.out.println("detected nodes: " + Long.toString(flag));
				}else {
					System.out.println("failed");
				}	
				i = i + 1;
                
			}
		}
        Job job3 = Job.getInstance(conf1, "Parallel Dijkstra");
		
        job3.setJarByClass(ParallelDijkstra.class);
		job3.setJar("paralleldijkstra.jar");
        job3.setMapperClass(PDPreProcess.ResultMapper.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
		Path outputJob3 = new Path(args[1]);
        FileInputFormat.addInputPath(job3, new Path(tmpPath + "/tmp" + Integer.toString(i)));
        FileOutputFormat.setOutputPath(job3, outputJob3);

		if (fs.exists(outputJob3)) {
					System.out.println("deleted folder: /final");
					fs.delete(outputJob3, true);
				}
		if(job3.waitForCompletion(true)){
			System.out.println("job3 completed, final result in output/");
			System.exit(0);
		}else{
			System.exit(1);
		}
	}
}