package br.com.alura.ecommerce;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/*
Replication in clusters
It is possible to run multiple kafka brokers, but in this case you should also
increase replication factor, so the data is copied to multiple brokers.
Every partition of a topic has a leader and a set of replicas. The leader is
where the write is done first, and then the data is replicated to the replicas.
If leader fails, one of the replicas becomes the new leader.
This is done to avoid having a single point of failure, that is we don't have
only one kafka broker, but multiple ones in case one of them goes down.
It is also a good practice to set topic offset and transaction state replication
factor to 3.
 */
public class HttpEcommerceService {
    public static void main(String[] args) throws Exception {
        var server = new Server(8080);

        var context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new NewOrderServlet()), "/new");

        // handles request through a context
        server.setHandler(context);

        server.start();
        // wait for server to finish and then finishes application
        server.join();
    }
}
