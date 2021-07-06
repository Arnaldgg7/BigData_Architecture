package bdma.labos.lambda.writers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class WriterClient {

	private DatagramSocket socket;
	private byte[] buffer;
	private InetAddress address;
	
	public WriterClient() throws SocketException, UnknownHostException {
		this.socket = new DatagramSocket();
		this.buffer = new byte[4096];
		this.address = InetAddress.getByName("master");
	}
	
	public void close() {
		this.socket.close();
	}
	
	public void write(byte[] data) {
		for (int i = 0; i < data.length; i++) {
			this.buffer[i] = data[i];
		}
		// ===> Where do you send the data? Into the master itself? Into HDFS?
		DatagramPacket packet = new DatagramPacket(this.buffer, data.length, this.address, 4444);
		try {
			this.socket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
