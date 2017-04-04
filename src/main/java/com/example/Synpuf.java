package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.opencsv.CSVParser;
import java.io.IOException;
import java.util.ArrayList;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import com.google.cloud.bigtable.dataflow.*;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Mutation;

public class Synpuf
{
	public static PCollection<String> lines;
	public static boolean isheader=true;
	public static ArrayList<String> header;
	public static ArrayList<ArrayList<String>> rows;
	public static ArrayList<String> row;
	static class ExtractFieldsFn extends DoFn<String, String> {
		@Override
    		public void processElement(ProcessContext c) throws IOException{
			
			rows=new ArrayList<ArrayList<String>>();	
			String line = c.element();
			CSVParser csvParser = new CSVParser();
 			String[] parts = csvParser.parseLine(line);
			if(isheader){
				isheader=false;
				header=new ArrayList<String>();	
				for(String part : parts){
					header.add(part);
					//System.out.println(part);
				}
     			}
      			else{
				row=new ArrayList<String>();	
      				for (String part : parts) {
					row.add(part);
        					c.output(part);
      				}
				rows.add(row);
			}
    		}
		
	}

	static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
 		 private static final long serialVersionUID = 1L;

		@Override
  		public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
    			//c.output(new Put(c.element().getBytes()).addColumn(FAMILY, QUALIFIER, VALUE));
			String family="sf-1";
			for(ArrayList row : rows){
				int index=0;
				for(String q: header){
					c.output(new Put(c.element().getBytes()).addColumn(family, q , row.get(0)));
					index++;
				}
			}
  		}
	};

	public static void main(String[] args) 
	{
		String projectId="healthcare-12";
		String instanceId="synpuf-01";
		 try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
		Admin admin = connection.getAdmin();
		HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("synpuf_beneficiary"));
      		descriptor.addFamily(new HColumnDescriptor("sf-1"));
		admin.createTable(descriptor);
		Table table = connection.getTable(TableName.valueOf("synpuf_beneficiary"));
		
		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
    		.withProjectId("healthcare-12")
    		.withInstanceId("synpuf-01")
    		.withTableId("synpuf_beneficiary")
    		.build();

		// Start by defining the options for the pipeline.
		
		DataflowPipelineOptions options = PipelineOptionsFactory.create()
    		.as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		
		// The 'gs' URI means that this is a Google Cloud Storage path
		options.setStagingLocation("gs://synpuf_data/staging1");

		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);
		CloudBigtableIO.initializeForWrite(p);

 		lines=p.apply(TextIO.Read.from("gs://synpuf_data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"));
     		lines.apply(ParDo.of(new ExtractFieldsFn()))
     		.apply(ParDo.of(MUTATION_TRANSFORM))
		.apply(CloudBigtableIO.writeToTable(config));

		p.run();
	
		}

	}

}