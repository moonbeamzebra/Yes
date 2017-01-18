package ca.magenta.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;


public class TCPServer implements Cloneable, Runnable {

    private static Logger logger = Logger.getLogger(ca.magenta.utils.TCPServer.class);

    private int port;

    private static ArrayList<ca.magenta.utils.TCPServer> launchedTCPServers = new ArrayList<ca.magenta.utils.TCPServer>();

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


    //private ArrayList<ca.magenta.utils.TCPServer> childTCPServers = new ArrayList<ca.magenta.utils.TCPServer>();
    private Thread runner = null;
    ServerSocket server = null;
    Socket data = null;
    protected volatile boolean shouldStop = false;

    protected Object myServlet = null;

    public synchronized void startServer() throws IOException {
        if (getRunner() == null) {
            logger.info(String.format("TCPServer port [%d]", port));
            server = new ServerSocket(port);
            setRunner(new Thread(this, name));
            getRunner().start();
            logger.debug("Runner for " + name + " thread started");
        }
    }

    public static void stopLaunchedTCPServers() {
        logger.info(String.format("stoplaunchedTCPServers size: [%d]", launchedTCPServers.size()));
        for (ca.magenta.utils.TCPServer childTCPServer : launchedTCPServers) {
            logger.info(String.format("In for [%s]", childTCPServer.getName()));
            childTCPServer.stopServer();
        }
    }

    public synchronized void stopServer() {
        if (server != null) {
            shouldStop = true;
            getRunner().interrupt();
            String rName = getRunner().getName();
            setRunner(null);
            try {
                server.close();
            } catch (IOException e) {
                logger.error("server IOException",e);
            }
            server = null;

            logger.info(String.format("TCPServer server [%s] stopped", rName));
        } else if (data != null) {
            shouldStop = true;
            getRunner().interrupt();
//            try {
//                getRunner().join(20000);
//            } catch (InterruptedException e) {
//                logger.error(String.format("InterruptedException", e));
//            }
            String rName = getRunner().getName();
            InetAddress clientAddress = data.getInetAddress();
            int port = data.getPort();
            try {
                data.close();
            } catch (IOException e) {
                logger.error(String.format("data IOException", e));
            }
            logger.info("TCPServer Disconnected to client: " + clientAddress.getHostAddress() + ":" + port);

            data = null;
            setRunner(null);
            logger.info(String.format("TCPServer data socket [%s] stopped", rName));
        }
    }

    public void run() {
        if (server != null) {
            while (!shouldStop) {
                try {
                    Socket datasocket = server.accept();
                    setClientCount(getClientCount() + 1);
                    TCPServer newTCPServer = null;
                    newTCPServer = (TCPServer) clone();
                    newTCPServer.server = null;
                    newTCPServer.data = datasocket;
                    launchedTCPServers.add(newTCPServer);
                    String nameStr = name + "-" + getClientCount();
                    newTCPServer.setRunner(new Thread(newTCPServer, nameStr));
                    newTCPServer.runner.start();
                    logger.debug("New client : Runner for " + nameStr + " thread started");
                } catch (IOException e) {
                    if (!shouldStop )
                        logger.error("IOException",e);
                } catch (CloneNotSupportedException e) {
                    logger.error("CloneNotSupportedException",e);
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

    public String getName() {
        return name;
    }

}