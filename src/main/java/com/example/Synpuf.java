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

import com.google.cloud.bigtable.hbase1_2.BigtableConnection;
import com.google.cloud.bigtable.dataflow.*;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.config.BigtableOptions;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.conf.Configuration;

public class Synpuf
{
	public static PCollection<String> lines;
	public static boolean isheader=true;
	public static ArrayList<String> header;
	public static ArrayList<String> row;
	
	/*static class ExtractFieldsFn extends DoFn<String, String> {
		@Override
    		public void processElement(ProcessContext c) throws IOException{
			
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
    		}
		
	}*/

	static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
 		 private static final long serialVersionUID = 1L;

		@Override
  		public void processElement(DoFn<String, Mutation>.ProcessContext c) throws IOException {
    			//c.output(new Put(c.element().getBytes()).addColumn(FAMILY, QUALIFIER, VALUE));
			
			/*for(ArrayList row : rows){
				int index=0;
				while(index<3){
					c.output(new Put(c.element().getBytes()).addColumn("sf-1".getBytes(), String.valueOf(header.get(index)).getBytes(), String.valueOf(row.get(index)).getBytes()));
					index++;
				}
			}*/
			
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
        			//c.output(part);
      				}
					int index=0;
					while(index<3){
						c.output(new Put(c.element().getBytes()).addColumn("sf-1".getBytes(), String.valueOf(header.get(index)).getBytes(), String.valueOf(row.get(index)).getBytes()));
						index++;
					}
    		}
  		}
	};

	public static void main(String[] args) throws IOException
	{
		String projectId="healthcare-12";
		String instanceId="synpuf-01";
		
		/*Configuration config1 = HBaseConfiguration.create();
		org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(config1);
		Admin admin = connection.getAdmin();
		
		HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("synpuf_beneficiary"));
      		descriptor.addFamily(new HColumnDescriptor("sf-1"));
		admin.createTable(descriptor);
		Table table = connection.getTable(TableName.valueOf("synpuf_beneficiary"));*/
		
		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
    		.withProjectId("healthcare-12")
    		.withInstanceId("synpuf-01")
    		.withTableId("synpuf-1")
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
     	lines.apply(ParDo.of(MUTATION_TRANSFORM))
		.apply(CloudBigtableIO.writeToTable(config));

		p.run();

	}

}