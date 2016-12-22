package ca.magenta.yes;

import ca.magenta.yes.connector.TcpConnector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class Application {

    final TcpConnector tcpConnector;

    public Application(TcpConnector tcpConnector) throws IOException {
        this.tcpConnector = tcpConnector;

        tcpConnector.startServer();

    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}