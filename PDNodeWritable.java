import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

public class PDNodeWritable implements Writable {
	public static int MAX_INT = 1000000000;

    private int node;
    private int distance;
    private int prev_node;
	private ArrayList<Map.Entry<Integer, Integer>> neighborList;

    public PDNodeWritable() { 
		this.node = -2;
		this.distance = MAX_INT;
		this.prev_node = -1;
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>();
    }
	public PDNodeWritable(int node){ 
		setNode(node);
		this.distance = MAX_INT;
		this.prev_node = -1;
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>();
	}
	public PDNodeWritable(int node, int distance, int prev_node ){ 
		setNode(node);
		setDistance(distance);
		setPrevNode(prev_node);
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>();
	}
    public PDNodeWritable(PDNodeWritable node){ 
		this.node = node.getNode();
		this.distance = node.getDistance();
		this.prev_node = node.getPrevNode();
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>(node.getNeighborList());
	}
	public int getNode(){
		return node;
	}
	public void setNode(int node){
		this.node = node;
	}

	public int getDistance(){
		return distance;
	}
	public void setDistance(int distance){
		this.distance = distance;
	}

	public int getPrevNode(){
		return prev_node;
	}
	public void setPrevNode(int prev_node){
		this.prev_node = prev_node;
	}
	public ArrayList<Map.Entry<Integer, Integer>> getNeighborList(){
		return neighborList;
	}
	public void setNeighborList(ArrayList<Map.Entry<Integer, Integer>> neighborList){
		this.neighborList = neighborList;
	}
	public void addNeighbor(int node, int weight){
		Map.Entry<Integer, Integer> tmp = new AbstractMap.SimpleEntry<>(node, weight);
		this.neighborList.add(tmp);
	}
	public void addNeighbor(int node){
		Map.Entry<Integer, Integer> tmp = new AbstractMap.SimpleEntry<>(node, 1);
		this.neighborList.add(tmp);
	}
	public String toString(){
		String output = Integer.toString(node) + " " + Integer.toString(distance)
			+ " " + Integer.toString(prev_node) + " " + Integer.toString(neighborList.size());
		for(int i = 0; i < neighborList.size(); ++i){
			Map.Entry<Integer, Integer> tmp = neighborList.get(i);
			output += " " + Integer.toString(tmp.getKey()) + " " + Integer.toString(tmp.getValue());
		}
		return output;
	}
	//add additional functions if needed
	public void write(DataOutput out) throws IOException {
    	out.writeInt(node);
		out.writeInt(distance);
		out.writeInt(prev_node);
		out.writeInt(neighborList.size());
		for(int i = 0; i < neighborList.size(); ++i){
			Map.Entry<Integer, Integer> tmp = neighborList.get(i);
			out.writeInt(tmp.getKey());
			out.writeInt(tmp.getValue());
		}
    }

    public void readFields(DataInput in) throws IOException {
		node = in.readInt();
		distance = in.readInt();
		prev_node = in.readInt();
		int size = in.readInt();
		this.neighborList = new ArrayList<Map.Entry<Integer, Integer>>();
		for(int i = 0; i < size; i++){
			int neighborNode = in.readInt();
			int weight = in.readInt();
			Map.Entry<Integer, Integer> tmp = new AbstractMap.SimpleEntry<>(neighborNode, weight);
			neighborList.add(tmp);
		}
    }
}