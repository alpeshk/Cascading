package com.bitwiseglobal.cascading.CascadingTrainingProject;

import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

import java.util.Iterator;
import java.util.ArrayList;

/**
 * Unit test for simple App.
 */
public class TransformInputTest extends CascadingTestCase
{
    
	@Test
    public void TransformInputTest()
    {
		/*TODO
		 * 1. Create a tuple list containing the data to be passed
		 * 2. Create a field list to pass to the TransformInput function
		 * 3. Create a Tuple List of expected results
		 * 4. Call TransformInput function
		 * 5. Add the results to the actualResults tuple list
		 * 6. Compare actual and expected results.
		 * */
		
		Fields inputFields = new Fields("SplitStringField","DateField")
									.applyTypes(String.class,String.class);
		Fields outputFields = new Fields("SplitString1","SplitString2","DateField")
				.applyTypes(String.class,String.class,String.class);
		
		Tuple [] arguments = new Tuple[6];
		arguments[0] = new Tuple("String1;String2","2015-11-20");
		arguments[1] = new Tuple("String1;","2015-06-11");
		arguments[2] = new Tuple(";String2","2016-12-11");
		arguments[3] = new Tuple(";","2016-12-28");
		arguments[4] = new Tuple(null,"2016-12-28");
		arguments[5] = new Tuple("String1;String2",null);
		
		ArrayList<Tuple> expectedResults = new ArrayList<Tuple>();
		expectedResults.add(new Tuple("String1","String2","11/20/2015"));
		expectedResults.add(new Tuple("String1","","06/11/2015"));
		expectedResults.add(new Tuple("","String2","12/11/2016"));
		expectedResults.add(new Tuple("","","12/28/2016"));
		expectedResults.add(new Tuple(null,null,"12/28/2016"));
		expectedResults.add(new Tuple("String1","String2",null));
		
		TupleListCollector collector = invokeFunction(new TransformInput(inputFields, "yyyy-MM-dd", "MM/dd/yyyy"), arguments, outputFields);
		
		Iterator<Tuple> itr = collector.iterator(); 
		ArrayList<Tuple> actualResults = new ArrayList<Tuple>();
		
		while(itr.hasNext()) {
			actualResults.add(itr.next());
		}
		
		assertEquals("Results do not match", expectedResults, actualResults);
       
    }

    }
