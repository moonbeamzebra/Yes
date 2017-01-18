package ca.magenta.yes;

import ca.magenta.yes.connector.ConnectorMgmt;
import ca.magenta.yes.connector.GenericConnector;
import ca.magenta.yes.connector.LogstashConnector;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import java.io.IOException;

@SpringBootApplication
public class Application {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Application.class.getPackage().getName());


    final ConnectorMgmt connectorMgmt;


    public Application(ConnectorMgmt connectorMgmt)  {
        this.connectorMgmt = connectorMgmt;

        Thread connectorMgmtThread = new Thread( this.connectorMgmt, "connectorMgmt");
    }

    public static void main(String[] args) throws IOException {

        ConfigurableApplicationContext cac = SpringApplication.run(Application.class, args);

        cac.addApplicationListener(new ApplicationListener<ContextClosedEvent>() {

            @Override
            public void onApplicationEvent(ContextClosedEvent event) {


                ConnectorMgmt connectorMgmt = (ConnectorMgmt) event.getApplicationContext().getBean("connectorMgmt");
                connectorMgmt.stop();

                logger.info(String.format("ContextClosedEvent: [%s]", event.toString()));

            }
        });
    }


}