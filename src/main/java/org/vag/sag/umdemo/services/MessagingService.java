package org.vag.sag.umdemo.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

@Service
public class MessagingService implements MessageListener{
    Logger logger = LoggerFactory.getLogger(MessagingService.class);

    @Value("${jms.destination.requestDestination}")
    private String requestDestinationName;

    @Value("${java.naming.factory.initial}")
    private String initialContext;

    @Value("${jms.provider.url}")
    private String jmsProviderURL;

    @Value("${jms.username}")
    private String jmsUsername;

    @Value("${jms.password}")
    private String jmsPassword;

    private Connection connection;
    private Session session;

    @PostConstruct
    public void initialize(){
        try {
            logger.info("Creating ConnectionFactory with" + ConnectionFactory.class);
            Properties properties = new Properties();
            properties.put("java.naming.factory.initial", initialContext);
            properties.put("java.naming.provider.url", jmsProviderURL);
            properties.put("java.naming.security.principal", jmsUsername);
            properties.put("java.naming.security.credentials", jmsPassword);
            Context context = new InitialContext(properties);

            ConnectionFactory connectionFactory
                    = (ConnectionFactory) context.lookup("MyUMConnectionFactorySD");

            connection = connectionFactory.createConnection();
            logger.info("Connection established " + connection.getClass());
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            logger.info("Session established " + session.getClass());
            Destination requestDestination = session.createQueue(requestDestinationName);
            logger.info("Destination lookup completed " + requestDestination.getClass());
            MessageConsumer consumer = session.createConsumer(requestDestination);
            consumer.setMessageListener(this);
            connection.start();
            logger.info("Listener initialized. Awaiting messages...");
        } catch (NamingException | JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onMessage(Message message) {
        String payload = null;
        try {
            logger.info("Message received - " + message.getJMSMessageID() + ", type=" + message.getClass() + ", CorrelationID=" + message.getJMSCorrelationID());

            if (message instanceof TextMessage) {
                payload = ((TextMessage) message).getText();
                logger.info(payload);
            }
            //ACK the received message manually because of the set Session.CLIENT_ACKNOWLEDGE above
            message.acknowledge();

            Destination responseDestination = message.getJMSReplyTo();
            logger.info("Preparing Response Message - " + responseDestination );
            MessageProducer producer = session.createProducer(responseDestination);
            TextMessage responseMessage = session.createTextMessage();
            responseMessage.setJMSCorrelationID(message.getJMSCorrelationID());
            responseMessage.setText("{\"OrderResponse\": { \"OrderStatus\": \"Success\"}}");
            producer.send(responseMessage);
            producer.close();
            logger.info("Response Message sent - " + responseMessage.getJMSMessageID() + ", type=" + responseMessage.getClass() + ", CorrelationID=" + responseMessage.getJMSCorrelationID());
        } catch (Exception ex) {
            logger.error("Error processing incoming message=" + payload, ex);
        }
    }
}
