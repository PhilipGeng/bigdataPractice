package polyu.bigdata.giraph;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

public class BreadthFirstSearch extends Vertex<LongWritable, DoubleWritable,
FloatWritable, DoubleWritable> {
	
	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
		
		if (getSuperstep() == 0) {
			/* Initialize vertex value to a very large double value */
			setValue(new DoubleWritable(Double.MAX_VALUE));
			if(getId().get()==0)
				setValue(new DoubleWritable(0));
			
		}
		// Your work here
		
		boolean changed = false;
		for (DoubleWritable msg : messages) {
			/* Collect messages from in-neighbours and update if necessary */
			if ( getValue().get() > (msg.get()+1) ) {
				setValue( new DoubleWritable( msg.get()+1 ) );
				changed = true;
			}
		}
		
		/* Send the message to out-neighbours at Superstep 0 or Vertex value is changed */
		if ( getSuperstep() == 0 || changed ) {
			sendMessageToAllEdges( getValue() );
		}
		voteToHalt();
	}

}

