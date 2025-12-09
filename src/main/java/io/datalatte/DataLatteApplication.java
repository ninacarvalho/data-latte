package io.datalatte;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The main entry point for the Data Latte Spring Boot application.
 *
 * The {@code @SpringBootApplication} annotation is a convenience annotation that adds all of the following:
 * <ul>
 *     <li>{@code @Configuration}: Tags the class as a source of bean definitions for the application context.</li>
 *     <li>{@code @EnableAutoConfiguration}: Tells Spring Boot to start adding beans based on classpath settings,
 *     other beans, and various property settings.</li>
 *     <li>{@code @ComponentScan}: Tells Spring to look for other components, configurations, and services in the
 *     {@code io.datalatte} package, allowing it to find and register them.</li>
 * </ul>
 */
@SpringBootApplication
public class DataLatteApplication {

    /**
     * The main method, which serves as the entry point for the Java application.
     * It delegates to Spring Boot's {@link SpringApplication} to bootstrap the application.
     *
     * @param args Command-line arguments passed to the application.
     */
    public static void main(String[] args) {
        // This call bootstraps the entire application, starting the embedded web server (if applicable),
        // scanning for components, and wiring up the application context.
        SpringApplication.run(DataLatteApplication.class, args);
    }
}
