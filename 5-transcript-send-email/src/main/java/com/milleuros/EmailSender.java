package com.milleuros;

import java.util.Properties;

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

public class EmailSender {
    private Properties properties ; 

    private final Session session ; 

    public EmailSender(String username, String password,boolean auth, boolean tlsEnable, String smtpHost, String smtpPort, String sslTrust){
        /* code from : https://www.baeldung.com/java-email */
        this.properties = new Properties();
        this.properties.put("mail.smtp.auth", auth);
        this.properties.put("mail.smtp.starttls.enable", tlsEnable);
        this.properties.put("mail.smtp.host", smtpHost);
        this.properties.put("mail.smtp.port", smtpPort);
        this.properties.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");

        this.session = Session.getInstance(this.properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });
    }

    public void sendEmail(String emailSender, String emailReceiver, String subject, String msg) throws Exception{

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(emailSender));
            message.setRecipients(
                    Message.RecipientType.TO,
                    InternetAddress.parse(emailReceiver)
            );
            message.setSubject(subject);
            message.setText(msg);

            Transport.send(message);

    }


}