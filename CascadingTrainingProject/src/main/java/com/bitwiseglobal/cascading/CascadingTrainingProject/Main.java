package com.bitwiseglobal.cascading.CascadingTrainingProject;

import java.sql.Date;
import java.util.Properties;

import com.google.protobuf.UnknownFieldSet.Field;
import com.sun.tools.internal.xjc.generator.bean.ImplStructureStrategy.Result;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.operation.expression.ExpressionFilter;
import cascading.flow.Flow;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.operation.AssertionLevel;
import cascading.operation.Filter;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;

/**
 * Hello world!
 *
 */
public class Main 
{
    public static void main( String[] args )
    {
    	String inPath = args[ 0 ];
    	String in2Path = args[ 1 ];
    	String outPath = args[ 2 ];
    	

        Properties properties = new Properties();
        AppProps.setApplicationJarClass( properties, Main.class );
        AppProps.setApplicationName( properties, "Cascading Training" );
        AppProps.addApplicationTag( properties, "tutorial:cascading" );
        AppProps.addApplicationTag( properties, "technology:Cascading" );
    	Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );
    	
    	Fields inputFields = new Fields("KeyField", "SplitStringField" ,"DateField" , "NumericField")
    							.applyTypes(Integer.class, String.class, String.class, Double.class );
    	Scheme InputScheme=new TextDelimited(inputFields,true,",","\"");

    	Tap inTap = new Hfs( InputScheme, inPath );

    	//Input file for Joins
    	
    	Fields input2Fields = new Fields("KeyField", "SplitStringField" ,"DateField" , "NumericField")
				.applyTypes(Integer.class, String.class, String.class, Double.class );
    	Scheme Input2Scheme=new TextDelimited(input2Fields,true,",","\"");

    	Tap in2Tap = new Hfs( Input2Scheme, in2Path );


/*    	Fields outfields=new Fields("KeyField","SplitString1","SplitString2","DateField","NumericField")
    			  .applyTypes(Integer.class,String.class,String.class,String.class,Double.class);*/
    	/*
    	Fields outfields=new Fields("KeyField","SplitString1","SplitString2","DateField","NumericField", "NumericField2")
  			  .applyTypes(Integer.class,String.class,String.class,String.class,Double.class, Double.class);
*/
    	Fields outfields=new Fields("KeyField", "SplitStringField" ,"DateField" , "NumericField", "SplitStringField2" ,"DateField2" , "NumericField2")
				.applyTypes(Integer.class, String.class, String.class, Double.class, String.class, String.class, Double.class );
    	
//    	Fields outfields=new Fields("KeyField", "SplitStringField" ,"DateField" , "NumericField")
//				.applyTypes(Integer.class, String.class, String.class, Double.class );
    	
    	Scheme outputScheme=new TextDelimited(outfields,true,"|","\"");

    	Tap outTap = new Hfs(outputScheme, outPath, SinkMode.REPLACE);
    	
    	Tap trapTap = new Hfs(new TextDelimited(true,"|"),outPath+"/trap", SinkMode.REPLACE);
    	
    	//ExpressionFilter filter = new ExpressionFilter("\\s+");
    	
    	/*
    	Fields checkInputFields = new Fields("KeyField", "NumericField");

        Pipe processPipe = new Each("processPipe", new CheckNullKey(inputFields), Fields.REPLACE);

    	
    	
//    	DateParser parser = new DateParser(new Fields("DateField").applyTypes(Date.class), "YYYY/MM/DD");
//    	processPipe = new Each( "processPipe", new Fields("DateField"), parser, Fields.REPLACE);
//    	DateFormatter formatter = new DateFormatter("MM/DD/YYYY");
//   	processPipe = new Each( "processPipe", new Fields("DateField"), formatter, Fields.REPLACE);
    	
    	
    	ExpressionFilter filter = new ExpressionFilter("NumericField >= 1000");
    	
    	
    	Fields numericField = new Fields("NumericField")
    								.applyTypes(Integer.class);
    	
    	processPipe = new Each("processPipe", numericField, filter);
    	
    	Fields TransformInputFields = new Fields("SplitStringField", "DateField")
				.applyTypes(String.class, Date.class);
    	Fields TransformOutputFields = new Fields ("SplitString1", "SplitString2", "DateField")
				.applyTypes(String.class, String.class, String.class);
    	
    	processPipe = new Each(processPipe, TransformInputFields, new TransformInput(TransformOutputFields, "yyyy-MM-dd", "MM/dd/yyyy"), Fields.SWAP );
    	
    	Fields joinField = new Fields("KeyField").applyTypes(Integer.class);
    	
    	Pipe input2Pipe = new Pipe("joinPipe");
*/    	
    	Pipe inputPipe = new Pipe("inputPipe");
    	Pipe input2Pipe = new Pipe("input2Pipe");
    	
    	Fields common = new Fields("KeyField");
    	Fields declared = new Fields("KeyField","SplitStringField" ,"DateField" , "NumericField","KeyField2", "SplitStringField2" ,"DateField2" , "NumericField2");
    	
    	Pipe coGroupPipe = new CoGroup(inputPipe, common, input2Pipe, common, declared , new InnerJoin());
    	
        coGroupPipe = new Retain(inputFields, common);
    	
        FlowDef flowDef = FlowDef.flowDef().setName( "Assignment 1" )
        				.addSource( inputPipe, inTap )
        				.addSource(input2Pipe, in2Tap)
        				.addTailSink( coGroupPipe, outTap );
//        				.addTrap(coGroupPipe, trapTap);
     // Run the flow
        Flow wcFlow = flowConnector.connect( flowDef );

        //flowDef.setAssertionLevel( AssertionLevel.VALID );

        wcFlow.complete();
    }
}
