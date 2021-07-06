package bdma.labos.lambda.writers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriterServer extends Thread {
	
	private Configuration config;
	private FileSystem fileSystem;
	private FSDataOutputStream writer;
	private DatagramSocket socket;
	
	
	
	private boolean running;
	// IMPORTANT: modify to your bdma user: e.g., bdma00
	private static final String FILENAME = "hdfs://master:27000/user/bdma17/lambda/"+String.valueOf(System.currentTimeMillis());
	
	public WriterServer() throws IOException {
		this.config = new Configuration();
		// IMPORTANT: modify to your bdma user: e.g., bdma00
		this.config.addResource(new Path("/home/bdma17/hadoop-2.7.4/etc/hadoop/core-site.xml"));
		this.fileSystem = FileSystem.get(config);
		this.writer = this.fileSystem.create(new Path(FILENAME));
		this.socket = new DatagramSocket(4444);
		
		this.running = true;
	}
	
	public void close() throws IOException {
		this.socket.close();
		this.writer.flush();
		this.writer.close();
		this.fileSystem.close();
	}
	
	public void finish() {
		this.running = false;
	}
	
	public void run() {
		try {
			byte[] buffer = new byte[4096];
			while (this.running) {
				DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
				socket.receive(packet);
				int bytes = packet.getLength();
				byte[] data = packet.getData();
				byte[] toWrite = new byte[bytes];
				for (int i = 0; i < bytes; i++) {
					toWrite[i] = data[i];
				}
				this.writer.write(toWrite);
			}
			this.close();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
