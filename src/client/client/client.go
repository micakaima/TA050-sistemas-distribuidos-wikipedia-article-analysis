package client

import (
	external "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/messages"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/parquet-go/parquet-go"
)

const connectionAttempts = 3
const connectionAttemptsDelayMs = 300

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputDir   string
	OutputDir  string
	BatchSize  uint32
}

type Client struct {
	conn    net.Conn
	running atomic.Bool
	config  ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}
	client := &Client{conn: conn, config: config}
	client.running.Store(true)
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	var err error
	var conn net.Conn

	for range connectionAttempts {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			slog.Warn("Retrying connection...")
			time.Sleep(connectionAttemptsDelayMs * time.Millisecond)
			continue
		}
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()
	go client.handleSignals()

	if err := client.sendWikiRecords(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	if err := client.recvQueriesAnswers(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	return nil
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.conn.Close()
}

func (client *Client) expectMsgType(expectedMsgType external.MsgCode) error {
	msgType, err := external.ReadMsgCode(client.conn)
	if err != nil {
		slog.Error("While reading message type", "err", err)
		return err
	}
	if msgType != expectedMsgType {
		return fmt.Errorf(UnexpectedMsgError)
	}
	return nil
}

func (client *Client) sendWikiRecords() error {
	files, err := os.ReadDir(client.config.InputDir)
	if err != nil {
		slog.Error("While reading input dir", "err", err)
		return err
	}
	for _, file := range files {
		filepath := filepath.Join(client.config.InputDir, file.Name())
		if err := client.sendFileRows(filepath); err != nil {
			slog.Error("While sending file rows", "err", err)
			return err
		}
	}
	if err := client.sendEOF(); err != nil {
		slog.Error("While sending EOF", "err", err)
		return err
	}
	return nil
}

func (client *Client) sendFileRows(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewReader(file)
	defer reader.Close()

	batch := make([]map[string]any, 0, client.config.BatchSize)
	for {
		recordMap := make(map[string]any)
		if err := reader.Read(&recordMap); err != nil {
			if err == io.EOF {
				return client.flushArticlesBatch(batch)
			}
			return err
		}
		batch = append(batch, recordMap)

		if len(batch) >= int(client.config.BatchSize) {
			if err := client.flushArticlesBatch(batch); err != nil {
				return err
			}
			batch = make([]map[string]any, 0, client.config.BatchSize)
		}

	}
}

func (client *Client) flushArticlesBatch(batch []map[string]any) error {
	if len(batch) > 0 {
		if err := external.WriteArticlesBatch(client.conn, batch); err != nil {
			return err
		}
		if err := client.expectMsgType(external.Ack); err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) sendEOF() error {
	if err := external.WriteEOF(client.conn); err != nil {
		return err
	}
	if err := client.expectMsgType(external.Ack); err != nil {
		return err
	}
	return nil
}

func (client *Client) recvQueriesAnswers() error {
	return nil
}
