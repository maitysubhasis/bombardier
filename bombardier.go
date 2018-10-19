package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	// "github.com/codesenberg/bombardier/internal"

	// "github.com/bombardier/internal"
	"github.com/cheggaaa/pb"
	fhist "github.com/codesenberg/concurrent/float64/histogram"
	uhist "github.com/codesenberg/concurrent/uint64/histogram"
	"github.com/maitysubhasis/bombardier/internal"
	uuid "github.com/satori/go.uuid"
)

type Change struct {
	fn   func()
	name string
}

type bombardier struct {
	bytesRead, bytesWritten int64

	// HTTP codes
	req1xx uint64
	req2xx uint64
	req3xx uint64
	req4xx uint64
	req5xx uint64
	others uint64

	conf        config
	barrier     completionBarrier
	ratelimiter limiter
	workers     sync.WaitGroup

	// su: ***
	timeTaken time.Duration
	latencies *uhist.Histogram
	requests  *fhist.Histogram

	client   client
	doneChan chan struct{}

	// RPS metrics
	rpl   sync.Mutex
	reqs  int64
	start time.Time

	// Errors
	errors *errorMap

	// Progress bar
	bar *pb.ProgressBar

	// Output
	out      io.Writer
	template *template.Template

	workersLock  sync.RWMutex
	numConnsLock sync.RWMutex

	continuteWorkers map[int64]bool
	workersCount     int64

	ready chan bool

	changes        []Change
	readyForChange bool
	currentChange  int
	// changeWorkerCount chan bool
}

func newBombardier(c config) (*bombardier, error) {
	if err := c.checkArgs(); err != nil {
		return nil, err
	}
	b := new(bombardier)
	b.conf = c
	b.latencies = uhist.Default()
	b.requests = fhist.Default()

	if b.conf.testType() == counted {
		b.bar = pb.New64(int64(*b.conf.numReqs))
	} else if b.conf.testType() == timed {
		b.bar = pb.New64(b.conf.duration.Nanoseconds() / 1e9)
		b.bar.ShowCounters = false
		b.bar.ShowPercent = false
	}
	b.bar.ManualUpdate = true

	if b.conf.testType() == counted {
		b.barrier = newCountingCompletionBarrier(*b.conf.numReqs)
	} else {
		b.barrier = newTimedCompletionBarrier(*b.conf.duration)
	}

	if b.conf.rate != nil {
		b.ratelimiter = newBucketLimiter(*b.conf.rate)
	} else {
		b.ratelimiter = &nooplimiter{}
	}

	b.out = os.Stdout

	tlsConfig, err := generateTLSConfig(c)
	if err != nil {
		return nil, err
	}

	var (
		pbody *string
		bsp   bodyStreamProducer
	)
	if c.stream {
		if c.bodyFilePath != "" {
			bsp = func() (io.ReadCloser, error) {
				return os.Open(c.bodyFilePath)
			}
		} else {
			bsp = func() (io.ReadCloser, error) {
				return ioutil.NopCloser(
					proxyReader{strings.NewReader(c.body)},
				), nil
			}
		}
	} else {
		pbody = &c.body
		if c.bodyFilePath != "" {
			var bodyBytes []byte
			bodyBytes, err = ioutil.ReadFile(c.bodyFilePath)
			if err != nil {
				return nil, err
			}
			sbody := string(bodyBytes)
			pbody = &sbody
		}
	}

	cc := &clientOpts{
		HTTP2:     false,
		maxConns:  c.numConns,
		timeout:   c.timeout,
		tlsConfig: tlsConfig,

		headers:      c.headers,
		url:          c.url,
		method:       c.method,
		body:         pbody,
		bodProd:      bsp,
		bytesRead:    &b.bytesRead,
		bytesWritten: &b.bytesWritten,
	}
	b.client = makeHTTPClient(c.clientType, cc)

	if !b.conf.printProgress {
		b.bar.Output = ioutil.Discard
		b.bar.NotPrint = true
	}

	b.template, err = b.prepareTemplate()
	if err != nil {
		return nil, err
	}

	// b.workers.Add(int(c.numConns))
	b.errors = newErrorMap()
	b.doneChan = make(chan struct{}, 2)

	b.continuteWorkers = make(map[int64]bool)
	b.ready = make(chan bool, 1)
	// b.changeWorkerCount = make(chan bool, 1)

	b.workersCount = 0

	b.changes = make([]Change, 0)
	b.currentChange = 0
	b.readyForChange = true
	b.ready <- true

	return b, nil
}

func makeHTTPClient(clientType clientTyp, cc *clientOpts) client {
	var cl client
	switch clientType {
	case nhttp1:
		cl = newHTTPClient(cc)
	case nhttp2:
		cc.HTTP2 = true
		cl = newHTTPClient(cc)
	case fhttp:
		fallthrough
	default:
		cl = newFastHTTPClient(cc)
	}
	return cl
}

func (b *bombardier) prepareTemplate() (*template.Template, error) {
	var (
		templateBytes []byte
		err           error
	)
	switch f := b.conf.format.(type) {
	case knownFormat:
		templateBytes = f.template()
	case userDefinedTemplate:
		templateBytes, err = ioutil.ReadFile(string(f))
		if err != nil {
			return nil, err
		}
	default:
		panic("format can't be nil at this point, this is a bug")
	}
	outputTemplate, err := template.New("output-template").
		Funcs(template.FuncMap{
			"WithLatencies": func() bool {
				return b.conf.printLatencies
			},
			"FormatBinary": formatBinary,
			"FormatTimeUs": formatTimeUs,
			"FormatTimeUsUint64": func(us uint64) string {
				return formatTimeUs(float64(us))
			},
			"FloatsToArray": func(ps ...float64) []float64 {
				return ps
			},
			"Multiply": func(num, coeff float64) float64 {
				return num * coeff
			},
			"StringToBytes": func(s string) []byte {
				return []byte(s)
			},
			"UUIDV1": uuid.NewV1,
			"UUIDV2": uuid.NewV2,
			"UUIDV3": uuid.NewV3,
			"UUIDV4": uuid.NewV4,
			"UUIDV5": uuid.NewV5,
		}).Parse(string(templateBytes))

	if err != nil {
		return nil, err
	}
	return outputTemplate, nil
}

func (b *bombardier) applyNextChange() {
	if b.readyForChange {
		if b.currentChange < len(b.changes) {
			b.readyForChange = false
			go func() {
				change := b.changes[b.currentChange]
				b.currentChange++
				change.fn()
				fmt.Println("Running change:", change.name)
			}()
		}
	}
}

func (b *bombardier) insertChange(change Change) {
	b.changes = append(b.changes, change)
	b.applyNextChange()
}

func (b *bombardier) writeStatistics(code int, msTaken uint64) {
	b.latencies.Increment(msTaken)
	b.rpl.Lock()
	b.reqs++
	b.rpl.Unlock()
	var counter *uint64
	switch code / 100 {
	case 1:
		counter = &b.req1xx
	case 2:
		counter = &b.req2xx
	case 3:
		counter = &b.req3xx
	case 4:
		counter = &b.req4xx
	case 5:
		counter = &b.req5xx
	default:
		counter = &b.others
	}

	atomic.AddUint64(counter, 1)
}

func (b *bombardier) performSingleRequest() {
	code, msTaken, err := b.client.do()
	if err != nil {
		b.errors.add(err)
	}
	// if code/100 < 1 {
	// 	fmt.Println(code, msTaken)
	// }
	b.writeStatistics(code, msTaken)
}

// su: each worker tries to send
func (b *bombardier) worker(workerID int64) {
	done := b.barrier.done()
	for b.barrier.tryGrabWork() {
		// if no limit is provided it never breaks
		if b.ratelimiter.pace(done) == brk { // √ ?
			break
		}
		b.performSingleRequest() // √
		b.barrier.jobDone()      // √

		if b.killWorker(workerID) {
			// atomic.AddInt64(&b.workersCount, -1)
			fmt.Println("Exit worker: ", workerID)
			break
		}
	}
}

func (b *bombardier) killWorker(workerID int64) bool {
	b.workersLock.Lock()
	ret := b.continuteWorkers[workerID] == false
	b.workersLock.Unlock()
	return ret

}

// su: impressive
func (b *bombardier) barUpdater() {
	done := b.barrier.done()
	for {
		select {
		case <-done:
			b.bar.Set64(b.bar.Total)
			b.bar.Update()
			b.bar.Finish()
			if b.conf.printProgress {
				fmt.Fprintln(b.out, "Done!")
			}
			b.doneChan <- struct{}{}
			return
		default:
			current := int64(b.barrier.completed() * float64(b.bar.Total))
			b.bar.Set64(current)
			b.bar.Update() // update progress
			time.Sleep(b.bar.RefreshRate)
		}
	}
}

// su: impressed
func (b *bombardier) rateMeter() {
	requestsInterval := 10 * time.Millisecond
	if b.conf.rate != nil {
		requestsInterval, _ = estimate(*b.conf.rate, rateLimitInterval)
	}
	requestsInterval += 10 * time.Millisecond
	ticker := time.NewTicker(requestsInterval)
	defer ticker.Stop()
	tick := ticker.C
	done := b.barrier.done()
	for {
		select {
		case <-tick:
			b.recordRps()
			continue
		case <-done:
			b.workers.Wait()
			b.recordRps()
			b.doneChan <- struct{}{}
			return
		}
	}
}

func (b *bombardier) recordRps() {
	b.rpl.Lock()
	duration := time.Since(b.start)
	reqs := b.reqs
	b.reqs = 0
	b.start = time.Now()
	b.rpl.Unlock()

	reqsf := float64(reqs) / duration.Seconds()
	b.requests.Increment(reqsf)
}

// final destination
func (b *bombardier) bombard() {
	if b.conf.printIntro {
		b.printIntro()
	}

	b.bar.Start()
	bombardmentBegin := time.Now()
	b.start = time.Now()

	b.addWorkers()

	go b.rateMeter()
	go b.barUpdater()
	b.workers.Wait()

	b.timeTaken = time.Since(bombardmentBegin)
	<-b.doneChan // su: waiting
	<-b.doneChan // su: waiting
}

// add new connections count
func (b *bombardier) addNumConns(numConns uint64) {
	b.numConnsLock.Lock()
	b.conf.numConns += numConns
	b.numConnsLock.Unlock()
}

// add real workers
func (b *bombardier) addWorkers() {
	count := int64(b.conf.numConns) - b.workersCount
	b.workers.Add(int(count))
	for i := count; i > 0; i-- {
		go func() {
			defer b.workers.Done()
			workerID := atomic.AddInt64(&b.workersCount, 1)
			if workerID == int64(b.conf.numConns) {
				fmt.Println("Now exec next change")
				b.readyForChange = true
				b.applyNextChange()
			}
			b.addWorkerID(workerID)
			b.worker(workerID)
		}()
	}
	// b.changeWorkerCount <- true
}

func (b *bombardier) removeWorkers(count uint64) {
	success := uint64(0)
	_count := int64(count)
	if b.conf.numConns > count {
		b.workersLock.Lock()
		for id := int64(0); id < _count; id++ {
			fmt.Println(b.workersCount-id, id)
			if _, ok := b.continuteWorkers[b.workersCount-id]; ok {
				success++
				b.continuteWorkers[b.workersCount-id] = false
			}
		}
		b.workersLock.Unlock()
		atomic.AddInt64(&b.workersCount, -_count)
		b.addNumConns(-success)
	}
	b.applyNextChange()
}

func (b *bombardier) addWorkerID(workerID int64) {
	b.workersLock.Lock()
	b.continuteWorkers[workerID] = true
	b.workersLock.Unlock()
}

func (b *bombardier) printIntro() {
	if b.conf.testType() == counted {
		fmt.Fprintf(b.out,
			"Bombarding %v with %v request(s) using %v connection(s)\n",
			b.conf.url, *b.conf.numReqs, b.conf.numConns)
	} else if b.conf.testType() == timed {
		fmt.Fprintf(b.out, "Bombarding %v for %v using %v connection(s)\n",
			b.conf.url, *b.conf.duration, b.conf.numConns)
	}
}

// su: this can be overwritten
func (b *bombardier) gatherInfo() internal.TestInfo {
	info := internal.TestInfo{
		Spec: internal.Spec{
			NumberOfConnections: b.conf.numConns,

			Method: b.conf.method,
			URL:    b.conf.url,

			Body:         b.conf.body,
			BodyFilePath: b.conf.bodyFilePath,

			CertPath: b.conf.certPath,
			KeyPath:  b.conf.keyPath,

			Stream:     b.conf.stream,
			Timeout:    b.conf.timeout,
			ClientType: internal.ClientType(b.conf.clientType),

			Rate: b.conf.rate,
		},
		Result: internal.Results{
			BytesRead:    b.bytesRead,
			BytesWritten: b.bytesWritten,
			TimeTaken:    b.timeTaken,

			Req1XX: b.req1xx,
			Req2XX: b.req2xx,
			Req3XX: b.req3xx,
			Req4XX: b.req4xx,
			Req5XX: b.req5xx,
			Others: b.others,

			Latencies: b.latencies,
			Requests:  b.requests,
		},
	}

	testType := b.conf.testType()
	info.Spec.TestType = internal.TestType(testType)
	if testType == timed {
		info.Spec.TestDuration = *b.conf.duration
	} else if testType == counted {
		info.Spec.NumberOfRequests = *b.conf.numReqs
	}

	if b.conf.headers != nil {
		for _, h := range *b.conf.headers {
			info.Spec.Headers = append(info.Spec.Headers,
				internal.Header{
					Key:   h.key,
					Value: h.value,
				})
		}
	}

	for _, ewc := range b.errors.byFrequency() {
		info.Result.Errors = append(info.Result.Errors,
			internal.ErrorWithCount{
				Error: ewc.error,
				Count: ewc.count,
			})
	}

	return info
}

func (b *bombardier) printStats() {
	info := b.gatherInfo()
	err := b.template.Execute(b.out, info)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func (b *bombardier) redirectOutputTo(out io.Writer) {
	b.bar.Output = out
	b.out = out
}

func (b *bombardier) disableOutput() {
	b.redirectOutputTo(ioutil.Discard)
	b.bar.NotPrint = true
}

func main() {
	cfg, err := parser.parse(os.Args)
	done := make(chan bool, 1)

	if err != nil {
		fmt.Println(err)
		os.Exit(exitFailure)
	}

	bombardier, err := newBombardier(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(exitFailure)
	}
	c := make(chan os.Signal, 1)

	cancel := func() {
		bombardier.barrier.cancel()
		done <- true
	}

	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()

	// 1. need systamatic connection creation and closing
	// 2. ramp up/down
	// 3.

	// su: more controlled bombarding is needed
	bombardier.insertChange(Change{
		func() {
			bombardier.bombard()
			done <- true
		},
		"bombard",
	})

	// <-bombardier.ready
	// fmt.Println("All workers are ready.")
	// time.Sleep(1 * time.Millisecond)
	// fn := func() {
	// 	bombardier.removeWorkers(800)
	// }
	// fmt.Println(fn)
	// bombardier.insertChange(Change{
	// 	func() {
	// 		bombardier.removeWorkers(90)
	// 	},
	// 	"removeWorkers",
	// })

	// time.Sleep(10 * time.Millisecond)
	// bombardier.addNumConns(2)
	// bombardier.addWorkers()

	<-done

	// su: need aggregate function from different bombardier
	if bombardier.conf.printResult {
		bombardier.printStats()
	}

	info := bombardier.gatherInfo()

	fmt.Println("")
	fmt.Println(bombardier.conf.numConns)
	fmt.Println(bombardier.requests.Count())
	info.Result.Requests.VisitAll(func(f float64, c uint64) bool {
		// fmt.Println(f, c)
		return true
	})

	info.Result.Latencies.VisitAll(func(f uint64, c uint64) bool {
		// fmt.Println(f, c)
		return true
	})
	// fmt.Println("")
	// fmt.Println(info.Result.Latencies.Count())
	// result := info.Result
	// fmt.Println("Total request:", result.Req1XX+result.Req2XX+result.Req3XX+result.Req4XX+result.Req5XX)
}

// ramp up, ramp down - sudden(done)
// ramp up, ramp down - over a period of time(???)
// total request (done)
// Req/s(done), Latency(done)
// avg(done), stdDev(done), Max(done)
// Http codes, -> done
// throughput -> done

// use internals/test_info.go for reference
