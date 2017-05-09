package ca.magenta.yes;

import ca.magenta.utils.AppException;
import ca.magenta.yes.ui.Customer;
import ca.magenta.yes.ui.CustomerRepository;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextClosedEvent;

import java.io.IOException;

@SpringBootApplication
public class Application {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Application.class.getPackage().getName());


    public Application(Globals globals) throws AppException {

        Globals.startEverything();

    }

    public static void main(String[] args) throws IOException {

        ConfigurableApplicationContext cac = SpringApplication.run(Application.class, args);

        cac.addApplicationListener(new ApplicationListener<ContextClosedEvent>() {

            @Override
            public void onApplicationEvent(ContextClosedEvent event) {

                Globals.stopEverything();

                logger.info(String.format("ContextClosedEvent: [%s]", event.toString()));

            }
        });
    }


//    // START Vaadin Things
//    @Bean
//    public CommandLineRunner loadData(CustomerRepository repository) {
//        return (args) -> {
//            // save a couple of customers
//            repository.save(new Customer("Jack", "Bauer"));
//            repository.save(new Customer("Chloe", "O'Brian"));
//            repository.save(new Customer("Kim", "Bauer"));
//            repository.save(new Customer("David", "Palmer"));
//            repository.save(new Customer("Michelle", "Dessler"));
//
//            // fetch all customers
//            logger.info("Customers found with findAll():");
//            logger.info("-------------------------------");
//            for (Customer customer : repository.findAll()) {
//                logger.info(customer.toString());
//            }
//            logger.info("");
//
//            // fetch an individual customer by ID
//            Customer customer = repository.findOne(1L);
//            logger.info("Customer found with findOne(1L):");
//            logger.info("--------------------------------");
//            logger.info(customer.toString());
//            logger.info("");
//
//            // fetch customers by last name
//            logger.info("Customer found with findByLastNameStartsWithIgnoreCase('Bauer'):");
//            logger.info("--------------------------------------------");
//            for (Customer bauer : repository
//                    .findByLastNameStartsWithIgnoreCase("Bauer")) {
//                logger.info(bauer.toString());
//            }
//            logger.info("");
//        };
//    }
//    // END Vaadin Things




}