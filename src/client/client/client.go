package client

import (
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

	external "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/codec"
	qresult "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	article "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/raw_article"
	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"

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
	done := make(chan struct{})
	client.handleSignals(done)
	defer close(done)

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

func (client *Client) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			slog.Info("SIGTERM signal received")
			client.running.Store(false)
			client.conn.Close()
		case <-done:
			return
		}
	}()
}

func (client *Client) expectMsgType(expectedMsgCode external.ExternalMsgCode) error {
	msgCode, err := external.ReadMsgCode(client.conn)
	if err != nil {
		slog.Error("While reading message type", "err", err)
		return err
	}
	if msgCode != expectedMsgCode {
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

	batch := article.NewEmptyArticlesBatch(int(client.config.BatchSize))
	for {
		rawArticle := make(article.RawArticle)
		if err := reader.Read(&rawArticle); err != nil {
			if err == io.EOF {
				return client.flushArticlesBatch(batch)
			}
			return err
		}
		batch.Add(&rawArticle)

		if batch.Len() >= int(client.config.BatchSize) {
			if err := client.flushArticlesBatch(batch); err != nil {
				return err
			}
			batch = article.NewEmptyArticlesBatch(int(client.config.BatchSize))
		}

	}
}

func (client *Client) flushArticlesBatch(batch article.RawArticlesBatch) error {
	if batch.Len() > 0 {
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
	for {
		msgCode, err := external.ReadMsgCode(client.conn)
		if err != nil {
			slog.Error("While reading message code")
			return err
		}

		switch msgCode {
		case external.QueryResult:
			if err := client.handleQueryResult(); err != nil {
				slog.Error("While handling Query Result message", "err", err)
				return err
			}
		case external.EOF:
			if err := client.handleEOF(); err != nil {
				slog.Error("While handling EOF message", "err", err)
				return err
			}
			return nil
		default:
			return fmt.Errorf(errors.UnknownExtMsgCode)
		}
	}
}

func (client *Client) handleQueryResult() error {
	queryResult, err := external.ReadQueryResult(client.conn)
	if err != nil {
		slog.Debug("While reading Query Result message")
		return err
	}
	if err := client.saveResult(queryResult); err != nil {
		slog.Debug("While saving query result")
		return err
	}
	return nil
}

func (client *Client) saveResult(queryResult qresult.QueryResult) error {
	path := client.generateOutputPath(queryResult)

	var resultWriter qresult.WritableResult
	switch queryResult.QueryCode {
	case qresult.Q1Code:
		q1Result, err := qresult.Q1ResultFromBytes(queryResult.ResultPayload)
		if err != nil {
			return err
		}
		resultWriter = q1Result

	case qresult.Q2Code:
		q2Result, err := qresult.Q2ResultFromBytes(queryResult.ResultPayload)
		if err != nil {
			return err
		}
		resultWriter = q2Result

	case qresult.Q5Code:
		q5Result, err := qresult.Q5ResultFromBytes(queryResult.ResultPayload)
		if err != nil {
			return err
		}
		resultWriter = q5Result

	default:
		return fmt.Errorf(errors.UnknownQueryResultCode)
	}
	return resultWriter.WriteToFile(path)
}

func (client *Client) handleEOF() error {
	if err := external.WriteAck(client.conn); err != nil {
		slog.Debug("While writing Ack message", "err", err)
		return err
	}
	return nil
}

func (client *Client) generateOutputPath(queryResult qresult.QueryResult) string {
	return filepath.Join(client.config.OutputDir, fmt.Sprintf("query_%v_result.txt", queryResult.QueryCode))
}
