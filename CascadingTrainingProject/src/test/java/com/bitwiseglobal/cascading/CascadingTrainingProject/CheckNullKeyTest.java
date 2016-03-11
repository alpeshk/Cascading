package com.bitwiseglobal.cascading.CascadingTrainingProject;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;
import cascading.CascadingTestCase;

import org.junit.Test;

import java.util.ArrayList; 
import java.util.Iterator; 
  

public class CheckNullKeyTest extends CascadingTestCase {

	@Test
	public void testCheckNullKey() {
		Fields fields = new Fields ("KeyField", "NumericField").applyTypes(Integer.class, Double.class);
		
		Tuple [] arguments = new Tuple[3];
		arguments[0] = new Tuple(1,50);
		arguments[1] = new Tuple(2,null);
		arguments[2] = new Tuple(3,3000);
		
		ArrayList<Tuple> expectedResults = new ArrayList<Tuple>();
		expectedResults.add(new Tuple(1,50.0));
		expectedResults.add(new Tuple(2,0.0));
		
		TupleListCollector collector = invokeFunction(new CheckNullKey(fields),arguments, Fields.ALL);
		Iterator<Tuple> itr = collector.iterator();
		ArrayList<Tuple> actualResults = new ArrayList<Tuple>();
		while(itr.hasNext()) {
				actualResults.add(itr.next());
		}
		assertEquals("Results do not match", expectedResults, actualResults);
		
	}

}
