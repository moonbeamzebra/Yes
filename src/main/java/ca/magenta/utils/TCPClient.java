package ca.magenta.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TCPClient implements Runnable {
	
	public PrintWriter getOutToServer() {
		return outToServer;
	}

	
	public TCPClient() {
		super();
	}

	Thread runner = null;
	Socket client = null;
	PrintWriter outToServer = null;
	BufferedReader inFromServer = null;
	protected volatile boolean shouldStop = false;
	

	public synchronized void startServer(String hostname, int port) throws IOException {
		if (runner == null) {
			client = new Socket(hostname, port);
			outToServer = new PrintWriter(client.getOutputStream(), true);
			inFromServer = new BufferedReader(new InputStreamReader(client.getInputStream()));
			runner = new Thread(this);
			runner.start();
		}
	}

	public synchronized void stopServer() {
		if (client != null) {
			shouldStop = true;
			runner.interrupt();
			runner = null;
			try {
				client.close();
			} catch (IOException ioe) {
			}
			client = null;
		}
	}

	public void run() {
		run(inFromServer, outToServer);
	}

	public void run(BufferedReader in, PrintWriter out) {
	}
}