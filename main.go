package main

/*
Outside Imports:
go get firebase.google.com/go
go get github.com/fatih/structs

*/

import (
	"context"
	"encoding/json"
	"firebase.google.com/go"
	"fmt"
	"google.golang.org/api/option"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type MessageThread struct {
	Name             string
	GroupChat        bool
	Participants     []string
	DateMessageCount map[string]int
	MessageCount     int
}

func (Thread *MessageThread) processJSON(CurrJSON jsonFile) {
	Thread.Name = CurrJSON.Title

	for _, participant := range CurrJSON.Participants {
		Thread.Participants = append(Thread.Participants, participant.Name)
	}

	Thread.DateMessageCount = make(map[string]int)
	Thread.MessageCount = 0
	for _, msg := range CurrJSON.Messages {
		t := time.Unix(int64(msg.TimestampMS/1000), int64(msg.TimestampMS%1000))
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		Thread.DateMessageCount[strconv.FormatInt(t.Unix(), 10)]++
		Thread.MessageCount++
	}

	if CurrJSON.ThreadType == "RegularGroup" {
		Thread.GroupChat = true
	} else {
		Thread.GroupChat = false
	}
}

type jsonFile struct {
	Participants       []participant
	Messages           []message
	Title              string
	IsStillParticipant bool   `json:"is_still_participant"`
	ThreadType         string `json:"thread_type"`
	ThreadPath         string `json:"thread_path"`
}

type message struct {
	SenderName  string `json:"sender_name"`
	TimestampMS int    `json:"timestamp_ms"`
	Content     string
	MessageType string `json:"message_type"`
}

type participant struct {
	Name string
}

func main() {
	start := time.Now()

	MessagesDir := os.Args[1]
	var Threads []MessageThread
	var CurrThread MessageThread
	var CurrJSON jsonFile

	err := filepath.Walk(MessagesDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if path[len(path)-12:] == "message.json" {
				CurrThread = MessageThread{}

				jsFile, err := os.Open(path)
				if err != nil {
					//TODO Add Error handling for file not existing
				} else {
					defer jsFile.Close()

					byteValue, _ := ioutil.ReadAll(jsFile)
					json.Unmarshal(byteValue, &CurrJSON)

					CurrThread.processJSON(CurrJSON)
					Threads = append(Threads, CurrThread)
				}
			}
			return nil
		})

	if err != nil {
		//TODO Add Error handling for FilePath walk failing
	}

	ctx := context.Background()
	sa := option.WithCredentialsFile("firebase-json.json")
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		log.Fatalln(err)
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Close()

	/*var dataset []map[string]interface{}
	var threadData map[string]interface{}
	var dateMessageCount map[string]interface{}

	for _, Thread := range Threads {
		threadData = map[string]interface{} {
			"name": Thread.Name,
			"groupChat": Thread.GroupChat,
			"participants": Thread.Participants,
			"messageCount": Thread.MessageCount,
			"dateMessageCount": Thread.DateMessageCount,
		}
		dataset = append(dataset, threadData)
	}*/
	_, _, err = client.Collection("MessengerDatasets").Add(ctx, map[string]interface{}{
		"name":    MessagesDir,
		"dataset": Threads,
	})

	elapsed := time.Since(start)
	fmt.Printf("Execution Time: %s", elapsed)
}
