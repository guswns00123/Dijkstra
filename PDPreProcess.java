import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PDPreProcess {
    
    public static class PreprocessMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            int start_node = Integer.parseInt(itr.nextToken());
            int finish_node = Integer.parseInt(itr.nextToken());
            int weight = Integer.parseInt(itr.nextToken());
            String edge = Integer.toString(finish_node) + " " + Integer.toString(weight);
            context.write(new IntWritable(start_node), new Text(edge));
        }
    }
    public static class ResultMapper extends Mapper<Object, Text, IntWritable, Text> {

        public static int MAX_INT = 1000000000;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            String tmp = itr.nextToken();
            int node = Integer.parseInt(itr.nextToken());
            int distance = Integer.parseInt(itr.nextToken());
            int prev_node = Integer.parseInt(itr.nextToken());

            if(distance < MAX_INT){
                String result_path = Integer.toString(distance);
                if(prev_node == -1){
                    result_path = result_path + " " + Integer.toString(node);
                }else{
                    result_path = result_path + " " + Integer.toString(prev_node);
                }
                context.write(new IntWritable(node), new Text(result_path));
            }
        }
    }
    public static class PreprocessReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            String output1 = Integer.toString(key.get()) + " " + "1000000000 -1 ";
            int countNeighbors = 0; // the number of connect of node
            String output2 = "";
            for(Text val : values){ // 2 7 , 3 20
                countNeighbors++;
                StringTokenizer itr = new StringTokenizer(val.toString());
                int neighbornode = Integer.parseInt(itr.nextToken()); //finish node
                int weight = Integer.parseInt(itr.nextToken()); //weight
                output2 = output2 + " " + Integer.toString(neighbornode) + " " + Integer.toString(weight);
            }
            String outputFinal = output1 + Integer.toString(countNeighbors) + output2;
            context.write(key, new Text(outputFinal)); //start_node max_num -1 countneighbor neighbor weight neighbor weight
        }
    }
}
