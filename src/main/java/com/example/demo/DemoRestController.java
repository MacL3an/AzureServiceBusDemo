package com.example.demo;

import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@RestController
public class DemoRestController {
    private QueueClient receiveClient;

    public DemoRestController() throws URISyntaxException {
        URI endpoint = new URI(System.getenv("ENDPOINT"));
        String sharedAccessKeyName = System.getenv("SHARED_ACCESS_KEY_NAME");
        String sharedAccessKey = System.getenv("SHARED_ACCESS_KEY");
        String entityPath = System.getenv("ENTITY_PATH");
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            // Just creating the QueueClient below gives us ERROR logs with com.microsoft.azure.servicebus.amqp.AmqpException after 60s
            receiveClient = new QueueClient(new ConnectionStringBuilder(endpoint, entityPath, sharedAccessKeyName, sharedAccessKey), ReceiveMode.PEEKLOCK);

            // If we also register a session handler we get ERROR logs with TimeOutExceptions after 30s
            // registerSessionHandler(receiveClient, executorService);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping(value = "/")
    public void hello() {
        System.out.println("Hello world!");
    }

    void registerSessionHandler(QueueClient queueClient, ExecutorService executorService) throws Exception {
        queueClient.registerSessionHandler(new ISessionHandler() {
                                               // callback invoked when the message handler loop has obtained a message
                                               public CompletableFuture<Void> onMessageAsync(final IMessageSession iMessageSession, IMessage message) {
                                                   System.out.println("MESSAGE RECEIVED");
                                                   System.out.println(message.getLabel());
                                                   return CompletableFuture.completedFuture(null);
                                               }

                                               @Override
                                               public CompletableFuture<Void> OnCloseSessionAsync(IMessageSession iMessageSession) {
                                                   return null;
                                               }

                                               // callback invoked when the message handler has an exception to report
                                               public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                                                   System.out.printf("ERROR: " + exceptionPhase + "-" + throwable.getMessage());
                                               }
                                           },
                new SessionHandlerOptions(1, 1, true, Duration.ofMinutes(1)),
                executorService);

    }
}