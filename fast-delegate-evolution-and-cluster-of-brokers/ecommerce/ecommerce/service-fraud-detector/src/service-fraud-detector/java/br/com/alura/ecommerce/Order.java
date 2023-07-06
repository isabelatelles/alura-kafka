package br.com.alura.ecommerce;

import java.math.BigDecimal;

/*
In one point of development, order didn't need email *for this service*, so we did not need to declare it
That is, the schema of consumer is loose, not strict, so in case the producer adds more attributes in
the message, it won't break consumer service
 */
public class Order {
    private final String orderId;
    private final BigDecimal amount;
    private final String email;

    public Order(String orderId, BigDecimal amount, String email) {
        this.orderId = orderId;
        this.amount = amount;
        this.email = email;
    }

    public String getEmail() {
        return email;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", amount=" + amount +
                ", email='" + email + '\'' +
                '}';
    }
}
