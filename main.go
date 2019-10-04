package main

import (
	"bytes"
	"github.com/Shopify/sarama"
	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"log"
)

const (
	IMAP_SERVER_URL      = ""
	IMAP_SERVER_USERNAME = ""
	IMAP_SERVER_PASSWORD = ""

	APACHE_KAFKA_URL   = ""
	APACHE_KAFKA_TOPIC = ""
)

func main() {

	//Connect to server
	c, err := client.DialTLS(IMAP_SERVER_URL, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Logout()

	//Login
	if err := c.Login(IMAP_SERVER_USERNAME, IMAP_SERVER_PASSWORD); err != nil {
		log.Fatal(err)
	}

	//Create Kafka SynProducer (for better performance is suggested AsyncProducer)
	brokers := []string{APACHE_KAFKA_URL}
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer producer.Close()

	// List mailboxes
	mailboxes := make(chan *imap.MailboxInfo, 10)
	done := make(chan error, 1)

	go func() {
		done <- c.List("", "*", mailboxes)
	}()

	if err := <-done; err != nil {
		log.Fatal(err)
	}

	// Select INBOX
	mbox, err := c.Select("INBOX", false)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Flags for INBOX:", mbox.Messages)

	from := uint32(1)
	to := mbox.Messages
	if mbox.Messages > 0 {
		// We're using unsigned integers here, only substract if the result is > 0
		//from = mbox.Messages
		from = 1
	}
	seqset := new(imap.SeqSet)
	seqset.AddRange(from, to)

	messages := make(chan *imap.Message, 10)
	done = make(chan error, 1)
	go func() {
		done <- c.Fetch(seqset, []imap.FetchItem{imap.FetchRFC822}, messages)
	}()

	counter := 0
	for email := range messages {
		counter++
		emailFormatted := email.Format()[1].(*bytes.Buffer)

		msg := &sarama.ProducerMessage{
			Topic: APACHE_KAFKA_TOPIC,
			Value: sarama.ByteEncoder(emailFormatted.Bytes()),
		}

		// Produce into kafka topic
		_, _, err := producer.SendMessage(msg)

		if err != nil {
			log.Printf("Failed to send message: %s", err)
		}
		log.Printf("EMAIL: %d", counter)
	}

	if err := <-done; err != nil {
		log.Fatal(err)
	}

	log.Println("Email were read successfully!")
}
