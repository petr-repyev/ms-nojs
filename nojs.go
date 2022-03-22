package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
)

// getGID return goroutine id (debug only)
func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

var (

	// HTTPDialTimeout timeout
	HTTPDialTimeout time.Duration

	// DomainNoErr OK status
	DomainNoErr = uint8(0)

	// DomainErrConnection Errro connecting to the host
	DomainErrConnection = uint8(1)

	// DomainErrHTTPStatus Bad HTTP response
	DomainErrHTTPStatus = uint8(2)

	// DomainErrRedirect Requestd URL has external redirects
	DomainErrRedirect = uint8(3)

	// DomainErrOther errors like: url.Parser, ...
	DomainErrOther = uint8(4)

	// DomainErrDNS404 err 404
	DomainErrDNS404 = uint8(5)

	// DomainErrDNStimeout timeout err
	DomainErrDNStimeout = uint8(6)

	// DomainErrDNStmp err
	DomainErrDNStmp = uint8(7)

	// DomainErrDNSerr unknown err
	DomainErrDNSerr = uint8(8)

	// DomainErrUnreach host in unreachable
	DomainErrUnreach = uint8(9)

	// DomainErrRefused connection refused
	DomainErrRefused = uint8(10)

	// DomainErrResetByPeer connection reset by peer
	DomainErrResetByPeer = uint8(11)

	// DomainErrSyscallUnknownError unknown error
	DomainErrSyscallUnknownError = uint8(12)

	// DomainErrOpUnknown other unknown error
	DomainErrOpUnknown = uint8(13)

	// @todo: in case add new errors, skip (100-200 - js render error)

	// HTTPError state flag
	HTTPError = uint8(2)

	// HTTPOk state flag
	HTTPOk = uint8(1)
)

// Beans queue
type Beans struct {

	// Mutex
	mtx sync.Mutex

	// Beanstalk connection
	c *beanstalk.Conn

	// Incoming tube
	tubeIn *beanstalk.TubeSet

	// Outgoing tube
	tubeOut *beanstalk.Tube
}

// Close queue connection
func (b *Beans) Close() error {
	return b.c.Close()
}

// Reserve incoming job
func (b *Beans) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	b.mtx.Lock()
	idv, bodyv, errv := b.tubeIn.Reserve(timeout)
	b.mtx.Unlock()
	return idv, bodyv, errv
}

// Put job into outgoing queue
func (b *Beans) Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {
	b.mtx.Lock()
	idv, errv := b.tubeOut.Put(body, pri, delay, ttr)
	b.mtx.Unlock()
	return idv, errv
}

// Config settings
type Config struct {

	// How manu cores will be used
	NumCPU int `yaml:"numcpu"`

	// Beanstalk(producer) queue settings
	Queue struct {

		// Connection string
		DSN string `yaml:"dsn"`

		// Incoming tube
		In string `yaml:"in"`

		// Outgoing tube
		Out string `yaml:"out"`
	} `yaml:"queue"`

	// Workers options
	Worker struct {

		// Num goroutines
		Num int `yaml:"num"`

		Useragent string `yaml:"useragent"`

		Timeouts struct {

			// General timeout (sec)
			Max time.Duration `yaml:"max"`

			// Connection timeout (sec)
			Connect time.Duration `yaml:"connect"`

			// Handshake timeout (sec)
			SSL time.Duration `yaml:"ssl"`

			// Idle connection timeout(micro)
			Idle time.Duration `yaml:"idle"`

			// Read headers (sec)
			Header time.Duration `yaml:"header"`

			// Expect continue (sec)
			Expect time.Duration `yaml:"expect"`

			// (TCP) Dial timeout (sec)
			Dial time.Duration `yaml:"dial"`

			// Keepalive timeout (sec)
			Keepalive time.Duration `yaml:"keepalive"`
		} `yaml:"timeouts"`

		// Use keepalive
		Keepalive bool `yaml:"keepalive"`

		// Max idle connections per host
		Perhost int `yaml:"perhost"`
	} `yaml:"worker"`

	// Storage (consumer) options
	Storage struct {

		// Num goroutines
		Num int `yaml:"num"`

		// Mongodb connection
		DSN string `yaml:"dsn"`

		// Database name
		DB string `yaml:"db"`

		// Connection name
		Name string `yaml:"name"`

		// Timeout
		Timeout time.Duration `yaml:"timeout"`
	} `yaml:"storage"`

	// Database (MySQL) settings
	Database struct {

		// Connection string
		DSN string `yaml:"dsn"`

		// Max open connnections
		Open int `yaml:"open"`

		// Max idle connections
		Idle int `yaml:"idle"`

		// Connection max TTL
		TTL time.Duration `yaml:"ttl"`
	} `yaml:"database"`
}

// HTTPResult entity
type HTTPResult struct {
	rid     int64
	status  uint8
	headers map[string]string
	err     uint8
	body    string
}

// Payload from Beanstalkd
type Payload struct {
	Id     uint64
	Domain string
	Tld    string
}

// Domain database entity
type Domain struct {
	id   uint64
	name string
	tld  string
	http HTTPResult
}

// RenderJob for JS tube
type RenderJob struct {
	DomainID   uint64
	DomainName string
	DomainTld  string
	RequestID  int64
}

// Result data
type Result struct {
	Rid     int64
	Headers map[string]string
	Body    string
}

// Job at queue
type Job struct {
	id     uint64
	domain Domain
}

// Custom TCP DialTimeout
func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, HTTPDialTimeout*time.Second)
}

// dialTLSTimeout timeout
func dialTLSTimeout(network, addr string) (net.Conn, error) {
	return tls.DialWithDialer(&net.Dialer{Timeout: HTTPDialTimeout * time.Second}, network, addr, &tls.Config{})
}

// NewHTTPClient Crawler's HTTP client
func NewHTTPClient(config *Config) *http.Client {

	var transport = &http.Transport{
		Dial:                  dialTimeout,
		DialTLS:               dialTLSTimeout,
		MaxIdleConns:          1,
		DisableKeepAlives:     config.Worker.Keepalive,
		MaxIdleConnsPerHost:   config.Worker.Perhost,
		TLSHandshakeTimeout:   config.Worker.Timeouts.SSL * time.Second,
		MaxConnsPerHost:       config.Worker.Perhost,
		IdleConnTimeout:       config.Worker.Timeouts.Idle * time.Microsecond,
		ResponseHeaderTimeout: config.Worker.Timeouts.Header * time.Second,
		ExpectContinueTimeout: config.Worker.Timeouts.Expect * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		TLSNextProto:          make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
	}

	HTTPClient := &http.Client{
		Timeout:   time.Second * config.Worker.Timeouts.Max,
		Transport: transport,
	}

	return HTTPClient
}

// LoadConfig returns a new decoded Config struct
func LoadConfig(configPath string) (*Config, error) {

	// Create config structure
	config := &Config{}

	// Open config file
	file, err := os.Open(configPath)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	// Init new YAML decode
	d := yaml.NewDecoder(file)

	// Start YAML decoding from file
	if err := d.Decode(&config); err != nil {
		return nil, err
	}

	return config, nil
}

// MySQLConnection new connection
func MySQLConnection(DSN string, ttl time.Duration, maxOpen int, maxIdle int) (*sql.DB, error) {

	db, err := sql.Open("mysql", DSN)

	if err != nil {
		panic(err)
	}

	// disabled for production (@todo move to ENV/cfg)
	// db.SetConnMaxLifetime(time.Second * ttl)

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	return db, nil
}

// MongoConnection new conection
func MongoConnection(DSN string) (*mongo.Client, error) {

	client, err := mongo.NewClient(options.Client().ApplyURI(DSN))

	if err != nil {
		panic(err)
	}

	return client, nil
}

// Main
func main() {

	// Do not check SSL
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	fmt.Println("Loading config")

	// Loading config
	config, err := LoadConfig("config.yml")

	if err != nil {
		panic(err)
	}

	// Limit SPU usage
	runtime.GOMAXPROCS(config.NumCPU)

	HTTPDialTimeout = config.Worker.Timeouts.Dial

	// Runtime context
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	// Worker results
	wOut := make(chan Job)

	// Beanstalkd. Storage queue
	queue, err := beanstalk.Dial("tcp", config.Queue.DSN)

	if err != nil {
		panic(err)
	}

	defer queue.Close()

	beans := Beans{
		c:       queue,
		tubeIn:  beanstalk.NewTubeSet(queue, config.Queue.In),
		tubeOut: beanstalk.NewTube(queue, config.Queue.Out),
	}

	// Database (MySQL)
	db, _ := MySQLConnection(config.Database.DSN, config.Database.TTL,
		config.Database.Open, config.Database.Idle)

	defer db.Close()

	fmt.Printf("Starting %d Srotage goroutine(s)\n", config.Storage.Num)

	// Storage
	for n := 1; n <= config.Storage.Num; n++ {
		g.Go(func() error {
			return Storage(ctx, db, &beans, config, wOut)
		})
	}

	fmt.Printf("Starting %d Workers goroutine(s)\n", config.Worker.Num)

	// Workers
	for n := 1; n <= config.Worker.Num; n++ {
		g.Go(func() error {
			return Worker(ctx, config, wOut)
		})
	}

	// OS Signal trap
	go func() {

		// OS Exit signals channel
		shutdownChannel := make(chan os.Signal, 1)

		// OS signals subscribtion
		signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

		<-shutdownChannel

		fmt.Println("Got termination signal, closing goroutines ...")

		// Finishing goroutines context
		cancel()
	}()

	// Workers errors handler
	if err := g.Wait(); err != nil {
		fmt.Printf("Goroutines error: %s\n", err)
		os.Exit(os.O_WRONLY)
	}

	fmt.Println("Exit")
}

// TCPErrCode metaspy TCP err codes
func TCPErrCode(err error) uint8 {

	switch errType := err.(*url.Error).Err.(type) {

	case *net.OpError:

		switch netErr := err.(*url.Error).Err.(*net.OpError).Unwrap().(type) {

		case *net.DNSError:

			dsnErr := err.(*url.Error).Err.(*net.OpError).Unwrap()

			if dsnErr.(*net.DNSError).IsTimeout {
				return DomainErrDNStimeout

			} else if dsnErr.(*net.DNSError).IsNotFound {
				return DomainErrDNS404

			} else if dsnErr.(*net.DNSError).IsTemporary {
				return DomainErrDNStmp

			} else {
				return DomainErrDNSerr
			}

		case *os.SyscallError:

			switch err.(*url.Error).Err.(*net.OpError).Err.(*os.SyscallError).Err.Error() {
			case "network is unreachable":
				return DomainErrUnreach

			case "connection refused":
				return DomainErrRefused

			case "connection reset by peer":
				return DomainErrResetByPeer

			default:
				return DomainErrSyscallUnknownError
			}

		default:
			_ = netErr
			return DomainErrOpUnknown
		}

	default:

		// @todo: catch everything below
		// http.httpError
		// x509.HostnameError
		// x509.CertificateInvalidError
		// x509.UnknownAuthorityError
		// *errors.errorString
		_ = errType
	}

	return DomainErrOpUnknown
}

// Worker goroutine
func Worker(ctx context.Context, config *Config, storage chan<- Job) error {

	var (
		payload Payload
		job     Job
	)

	HTTPClient := NewHTTPClient(config)

	MQ, err := beanstalk.Dial("tcp", config.Queue.DSN)

	if err != nil {
		return fmt.Errorf("Queue connection error (%s)", err.Error())
	}

	defer MQ.Close()

	tubeIn := beanstalk.NewTubeSet(MQ, config.Queue.In)

	for {

		// Defaults
		job = Job{}

		// Be nice to CPU
		time.Sleep(10 * time.Millisecond)

		select {

		// Gracefull shutdown
		case <-ctx.Done():
			return ctx.Err()

		// Worker logic here ...
		default:

			// Pick up a job (release time 3 times more largest http timeout)
			id, body, err := tubeIn.Reserve(config.Worker.Timeouts.Max * 3 * time.Second)

			if err != nil {

				// @todo use sleep, make graceful continue
				return fmt.Errorf("Queue reserve panic. quit (%s)", err.Error())
			}

			// HTTP request
			json.Unmarshal(body, &payload)

			job = Job{id: id, domain: Domain{id: payload.Id, name: payload.Domain, tld: payload.Tld}}

			job.domain.http = HTTPResult{status: uint8(0), headers: make(map[string]string), err: uint8(0), body: ""}

			startHost := fmt.Sprintf("%s.%s", job.domain.name, job.domain.tld)

			startURL := fmt.Sprintf("http://%s", startHost)

			req, err := http.NewRequest("GET", startURL, nil)

			if err != nil {

				// @todo workout retry attempt
				job.domain.http.status = DomainErrOther

				err = MQ.Delete(id)

				if err != nil {
					return fmt.Errorf("Storage beanstalk delete panic(%s, id=%d)", err.Error(), id)
				}

				select {
				case storage <- job:
					break

				case <-ctx.Done():
					return ctx.Err()
				}

				continue
			}

			req.Header.Set("Cache-Control", "max-age=0")
			req.Header.Set("Connection", "close")
			req.Header.Set("DNT", "1")
			req.Header.Set("Upgrade-Insecure-Requests", "1")
			req.Header.Set("User-Agent", config.Worker.Useragent)
			req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
			req.Header.Set("Accept-Encoding", "gzip")
			req.Header.Set("Accept-Language", "ru,en-US;q=0.9,en;q=0.8,la;q=0.7")

			resp, err := HTTPClient.Do(req)

			if err != nil {

				// @todo: workout retry attempt
				job.domain.http.status = HTTPError
				job.domain.http.err = TCPErrCode(err)

				err = MQ.Delete(id)

				if err != nil {
					return fmt.Errorf("Storage beanstalk delete panic(%s, id=%d)", err.Error(), id)
				}

				select {
				case storage <- job:
					break

				case <-ctx.Done():
					return ctx.Err()
				}

				continue
			}

			// Discard body
			// io.Copy(ioutil.Discard, resp.Body)
			// @see https://coderwall.com/p/3t9ljq/discard-response-body-in-go-http-request

			// Read only 1Mb of body (mind zip bombs)
			body1M, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1024*1024))

			if err == nil {
				job.domain.http.body = string(body1M)
			}

			// force close TCP!!!
			resp.Body.Close()

			HTTPClient.CloseIdleConnections()

			// Status
			if resp.StatusCode != http.StatusOK {

				job.domain.http.status = HTTPError
				job.domain.http.err = DomainErrHTTPStatus

			} else {

				// Final URL in case printl redirects
				endURL := resp.Request.URL.String()

				endURLObj, err := url.Parse(endURL)

				// Check final URL has same Host
				if err != nil {

					job.domain.http.err = DomainErrOther

					// @todo: WTF? Punycode?
					job.domain.http.status = HTTPError

				} else {

					// Canonical (no 'www.' + uppercase)
					endHost := strings.ToUpper(endURLObj.Host)

					if endURLObj.Port() != "" {
						noPort := ":" + endURLObj.Port()
						endHost = strings.ReplaceAll(endHost, noPort, "")
					}

					if strings.HasPrefix(endHost, "WWW.") {
						endHost = endHost[4:]
					}

					if startHost != endHost {
						job.domain.http.err = DomainErrRedirect
						job.domain.http.status = HTTPError

					} else {
						job.domain.http.status = HTTPOk
						job.domain.http.err = DomainNoErr
					}
				}

			} // Status OK

			// Headers
			for k, v := range resp.Header {
				job.domain.http.headers[k] = v[0]
			}

			err = MQ.Delete(id)

			if err != nil {
				return fmt.Errorf("Storage beanstalk delete panic(%s, id=%d)", err.Error(), id)
			}

			select {
			case storage <- job:
				break

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// Storage goroutine
func Storage(ctx context.Context, db *sql.DB, beans *Beans, config *Config, worker <-chan Job) error {

	var (
		// Saved Mysql request ID
		requestID int64

		// Result job
		job Job
	)

	// Mongo Database
	MongoClient, _ := MongoConnection(config.Storage.DSN)

	mgctx, cancel := context.WithTimeout(context.Background(), config.Storage.Timeout*time.Second)

	defer cancel()

	err := MongoClient.Connect(mgctx)

	if err != nil {
		return fmt.Errorf("Storage mongo connect panic. quit (%s)", err.Error())
	}

	collection := MongoClient.Database(config.Storage.DB).Collection(config.Storage.Name)

	defer MongoClient.Disconnect(mgctx)

	for {

		// Be nice to CPU
		time.Sleep(10 * time.Millisecond)

		// Defaults
		job = Job{}

		select {

		// Gracefull shutdown
		case <-ctx.Done():
			return ctx.Err()

		// Hard work here ...
		default:

			select {

			// Picking up a job
			case job = <-worker:
				break

			// Gracefull shutdown
			case <-ctx.Done():
				return ctx.Err()
			}

			// Save request
			sql := `INSERT INTO request ( domain_id, created, status, err ) 
					VALUES ( ?, UNIX_TIMESTAMP(), ?, ? )`

			res, err := db.Exec(sql, job.domain.id, job.domain.http.status, job.domain.http.err)

			if err != nil {
				return fmt.Errorf("Storage MySQL exec panic. quit (%s)", err.Error())
			}

			requestID, err = res.LastInsertId()

			if err != nil {
				return fmt.Errorf("Storage Mysql last insert panic. quit (%s)", err.Error())
			}

			job.domain.http.rid = requestID

			// defer insert.Close()

			// Saving response headers to mongodb
			// and put as job into render queue
			if job.domain.http.status == HTTPOk {

				result := Result{job.domain.http.rid, job.domain.http.headers, job.domain.http.body}

				_, err := collection.InsertOne(context.TODO(), result)

				if err != nil {
					return fmt.Errorf("Storage mongo insert panic. quit (%s)", err.Error())
				}

				// Reduce mem usage
				job.domain.http.headers = nil
				job.domain.http.body = ""

				// &RenderJob memleak? :/
				payload, err := json.Marshal(&RenderJob{
					DomainID:   job.domain.id,
					DomainName: job.domain.name,
					DomainTld:  job.domain.tld,
					RequestID:  job.domain.http.rid})

				if err != nil {
					return fmt.Errorf("Storage json panic (%s)", err.Error())
				}

				_, err = beans.Put([]byte(payload), 1, 0, 10*time.Minute)

				if err != nil {
					return fmt.Errorf("Storage beanstalk insert panic (%s)", err.Error())
				}

			} // HTTP Status == OK

		} // Ctx

	} // Main Loop
}
