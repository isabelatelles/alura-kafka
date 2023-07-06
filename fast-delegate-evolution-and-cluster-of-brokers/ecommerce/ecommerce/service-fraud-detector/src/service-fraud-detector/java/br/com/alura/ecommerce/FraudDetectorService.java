package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

/*
In each partition, messages are ordered;
Partitions are divided among each UP instance of the service (consumer),
so it doesn't matter if you have multiple instances up but
fewer partitions than instances
Example:
3 instances and 3 partitions => each consumer has 1 partition
3 instances and 6 partitions => each consumer has 2 partitions
3 instances and 1 partition => one consumer has 1 partition, the others have none
N_PARTITIONS >= N_CONSUMERS_INSIDE_A_GROUP

When a new instance is started, Kafka will rebalance the partitions among instances;
Messages are sent to a specific partition depending on their keys, i.e. a hash function
is applied to the key and it will assign it to a specific partition

From parallelization test:
"The key is used to distribute the message across existing partitions and consequently across instances of a service within a consumer group."
*/
public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                new HashMap<String, String>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for fraud.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Consumer is also a producer
        var order = record.value();
        if (isFraud(order)) {
            System.out.println("Order is a fraud!!! " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }

    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
