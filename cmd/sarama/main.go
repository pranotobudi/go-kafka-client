package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/labstack/echo/v4"
)

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"0.0.0.0:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

type Comment struct {
	Text string `form:"text" json:"text"`
}
type response struct {
	success bool   `json:"success"`
	message string `json:"message"`
	comment string `json:"comment"`
}

func createComment(c echo.Context) error {
	// Instantiate new Message struct
	cmt := new(Comment)
	if err := c.Bind(cmt); err != nil {
		fmt.Println("cmt error:", cmt)
		res := response{
			success: false,
			message: err.Error(),
		}
		c.JSON(http.StatusBadRequest, res)
		return err
	}
	fmt.Println("cmt:", *cmt)
	// convert body into bytes and send it to kafka
	cmtInBytes, err := json.Marshal(*cmt)
	PushCommentToQueue("comments", cmtInBytes)
	// Return Comment in JSON format
	res := response{
		success: true,
		message: "Comment pushed successfully",
		comment: cmt.Text,
	}
	c.JSON(http.StatusOK, res)
	if err != nil {
		res := response{
			success: false,
			message: "Error creating product",
		}
		c.JSON(http.StatusGatewayTimeout, res)
		return err
	}
	return err
}

func main() {
	app := echo.New()
	// api := app.Group("/api/v1")
	app.POST("/api/v1/comment", createComment)
	app.Start(":3000")
}
