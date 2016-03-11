package com.bitwiseglobal.cascading.CascadingTrainingProject;

import java.time.LocalDate;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;


//import com.google.protobuf.TextFormat.ParseException;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import java.text.SimpleDateFormat;


public class TransformInput  extends BaseOperation implements Function {
	String inFormat;
	String outFormat;
	
	  public TransformInput( Fields fieldDeclaration , String inFormat, String outFormat)
	    {
	    super( 2, fieldDeclaration );
	    this.inFormat = inFormat;
	    this.outFormat = outFormat;
	    }

	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
	    TupleEntry argument = functionCall.getArguments();
	    String SplitStringField = argument.getString( 0 );
	    String DateField = argument.getString( 1 );
	    SimpleDateFormat inDateFormat = new SimpleDateFormat(this.inFormat);
	    SimpleDateFormat outDateFormat = new SimpleDateFormat(this.outFormat);
	    
	    String [] SplitStrings;
	    String SplitString1 = null;
	    String SplitString2 = null;

	    
	    try {
	    	//System.out.println(DateField);
	    	if(DateField != null) {
			 Date date = inDateFormat.parse(DateField);
			 //System.out.println(date);
			 DateField = outDateFormat.format(date);
	    	}
			 
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    
		
		Tuple result = new Tuple();
	    
		if(SplitStringField !=null) {
			SplitStrings = SplitStringField.split("\\;", -1);
		    SplitString1 = SplitStrings[0];
		    SplitString2 = SplitStrings[1];
	    } 

	      result.add(SplitString1);
	      result.add(SplitString2);
	      result.add(DateField);
	      functionCall.getOutputCollector().add( result );
		
	    
	}
}
