package br.com.alura.ecommerce;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    // similar to dependency injection, we're initializing dispatchers only once, reusing it in any piece
    // of code of this service, and then destroying it
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    // HTTP GET
    /*
    The recommended is the miniminal of code possible in the input end.
    The less code at the input end, the faster you delegate to the messaging system,
    the faster you send this message, the less chance an error here. If you need to replicate
    the entire process, it is also easier to do that while just replaying the message of a new order.
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            // not concerned regarding security issues, this is only a starting point
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = UUID.randomUUID().toString();
            var order = new Order(orderId, amount, email);

            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

            // How much code should we keep on http ecommerce server? isn't sending an email a simple
            // action that could be performed here instead of sending a message to a topic to another
            // service that will send the email?
            // Well, the more code I put here, the greater the chance of throwing an exception,
            // the greater the chance of giving an error and something not working and nobody knowing
            // what happened. While I don't send the message, I don't have an easy way to replicate the
            // entire process.
            var emailCode = "Thank you for your order! We are processing it";
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);

            System.out.println("New order sent successfully.");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order was sent.");
        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
