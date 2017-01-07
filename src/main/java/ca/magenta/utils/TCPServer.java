package ca.magenta.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


public class TCPServer implements Cloneable, Runnable {
	
	private static Logger logger = Logger.getLogger(ca.magenta.utils.TCPServer.class);
	
	private int port;
	private String name = "TCPServer";
	
	
	public int getClientCount() {
		return clientCount;
	}

	public void setClientCount(int clientCount) {
		this.clientCount = clientCount;
	}

	private static int clientCount = 0;

	public TCPServer(int port, String name) {
		this.port = port;
		this.name = name;
	}

	private Thread runner = null;
	ServerSocket server = null;
	Socket data = null;
	volatile boolean shouldStop = false;
	
	protected Object myServlet = null;

	public synchronized void startServer() throws IOException {
		if (getRunner() == null) {
			logger.info(String.format("TCPServer port [%d]", port));
			server = new ServerSocket(port);
			setRunner(new Thread(this,name));
			getRunner().start();
			logger.debug("Runner for " + name + " thread started");
		}
	}

	public synchronized void stopServer() {
		if (server != null) {
			shouldStop = true;
			getRunner().interrupt();
			setRunner(null);
			try {
				server.close();
			} catch (IOException ioe) {
			}
			server = null;
		}
	}

	public void run() {
		if (server != null) {
			while (!shouldStop) {
				try {
					Socket datasocket = server.accept();
					setClientCount(getClientCount()+1);
					ca.magenta.utils.TCPServer newSocket = (ca.magenta.utils.TCPServer) clone();
					newSocket.server = null;
					newSocket.data = datasocket;
					String nameStr = name + "-" + getClientCount();
					newSocket.setRunner(new Thread(newSocket,nameStr));
					newSocket.getRunner().start();
					logger.debug("New client : Runner for " + nameStr + " thread started");
				} catch (Exception e) {
				}
			}
		} else {
			run(data);
		}
	}

	public void run(Socket data) {
	}

	public Thread getRunner() {
		return runner;
	}

	public void setRunner(Thread runner) {
		this.runner = runner;
	}
}