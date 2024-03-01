import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import configparser


def send_mail_alert(subject,message_body):
    # Read configuration from config.ini file
    config = configparser.ConfigParser()
    config.read('config/config.ini')

    # Email configuration
    sender_email = config.get('Email', 'sender_email')
    receiver_emails = config.get('Email', 'receiver_emails').split(',')
    
    # Create a multipart message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ', '.join(receiver_emails)
    message['Subject'] = subject

    # Add message body
    message.attach(MIMEText(message_body, 'plain'))

    # SMTP server configuration (for Gmail)
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587  # Port for TLS connection

    # Start TLS (Transport Layer Security) connection
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()

    # Login to Gmail SMTP server
    gmail_password = config.get('Email', 'gmail_password')
    server.login(sender_email, gmail_password)

    # Send email
    server.sendmail(sender_email, receiver_emails, message.as_string())

    # Close SMTP connection
    server.quit()