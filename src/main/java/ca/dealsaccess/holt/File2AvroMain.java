package ca.dealsaccess.holt;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class File2AvroMain {

	private int numBatch = 100;
	
	public static void main(String[] args) throws IOException {
		File2AvroMain main = new File2AvroMain();
		main.run(args);
	}

	private void run(String[] args) throws IOException {
		if(args.length < 3) {
			System.out.println("Usage: RPCClientMain <host> <port> <directory> <numBatch>");
			System.exit(1);
		}
		
		String host = args[0];
		String port = args[1];
		String directory = args[2];
		
		if(args.length == 4) {
			numBatch = Integer.parseInt(args[3]);
		}
		
		MyRpcClientFacade client = new MyRpcClientFacade();
		// Initialize client with the remote Flume agent's host and port
		client.init(host, Integer.parseInt(port));

		File dir = new File(directory);
		List<String> records = new ArrayList<String>();
		
		if(dir.isFile() || dir.listFiles().length <= 0) {
			System.out.println("this path has no files or is not a directory");
		}
		for(File f : dir.listFiles()) {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String record = null;
			int i = 0;
			while((record = br.readLine()) != null) {
				System.out.println(record);
				records.add(record);
				i++;
				if(i % numBatch == 0) {
					client.sendDataToFlume(records);
					records.clear();
				}
			}
			br.close();
		}
		
		client.cleanUp();
		
	}
}

class MyRpcClientFacade {
	private RpcClient client;
	private String hostname;
	private int port;

	public void init(String hostname, int port) {
		// Setup the RPC connection
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
		// Use the following method to create a thrift client (instead of the
		// above line):
		// this.client = RpcClientFactory.getThriftInstance(hostname, port);
	}

	public void sendDataToFlume(List<String> records) {
		List<Event> events = new ArrayList<Event>();
		for(String record : records) {
			events.add(EventBuilder.withBody(record, Charset.forName("UTF-8")));
		}
		// Create a Flume Event object that encapsulates the sample data
		//Event event = EventBuilder.withBody(records, Charset.forName("UTF-8"));

		// Send the event
		try {
			client.appendBatch(events);
		} catch (EventDeliveryException e) {
			// clean up and recreate the client
			client.close();
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
			// Use the following method to create a thrift client (instead of
			// the above line):
			// this.client = RpcClientFactory.getThriftInstance(hostname, port);
		}
	}

	public void cleanUp() {
		// Close the RPC connection
		client.close();
	}

}