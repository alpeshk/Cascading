package com.bitwiseglobal.cascading.CascadingTrainingProject;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CheckNullKey extends BaseOperation implements Function {
	  public CheckNullKey( Fields fieldDeclaration )
	    {
	    super( 2, fieldDeclaration );
	    }

	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
	    TupleEntry argument = functionCall.getArguments();
	    String KeyField = argument.getString( 0 );
	    /*String SplitStringField = argument.getString( 1 );
	    String DateField = argument.getString( 2 );*/
	    Double NumericField = argument.getDouble( 1 );
	    

	    if( !KeyField.isEmpty() && !NumericField.isNaN())
	      {
	      Tuple result = new Tuple();
	      result.add( Integer.parseInt(KeyField ));
	      /*result.add(SplitStringField);
	      result.add(DateField);*/
	      result.add(NumericField);
	      functionCall.getOutputCollector().add( result );
	      }
		
	}
	
}
