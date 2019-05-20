package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micro/mdns"
	"github.com/valyala/fasthttp"
	"github.com/valyala/ybc/bindings/go/ybc"
)

type cacheServer struct {
	originMapMu             sync.RWMutex
	originMap               map[string]string
	cache                   ybc.Cacher
	stats                   Stats
	upstreamClient          *fasthttp.Client
	logger                  *log.Logger
	upstreamHostBytes       []byte
	keyPool                 sync.Pool
	entriesChan             chan *mdns.ServiceEntry
	queryInterval           time.Duration
	httpSrv                 *fasthttp.Server
	queryDoneChan, quitChan chan struct{}
}

// NewCacheServer returns a new instance of a cache server.
func NewCacheServer(l *log.Logger, queryInt time.Duration) *cacheServer {
	c := &cacheServer{
		cache:          createCache(l),
		upstreamClient: &fasthttp.Client{},
		entriesChan:    make(chan *mdns.ServiceEntry),
		queryInterval:  queryInt,
		logger:         l,
		originMap:      make(map[string]string),
		queryDoneChan:  make(chan struct{}),
		quitChan:       make(chan struct{}),
	}
	return c
}

// Start starts the cache server.
func (cs *cacheServer) Start() {
	var ln net.Listener
	cs.httpSrv, ln = cs.serveHttp(*listenAddr)
	go func() {
		if cs.httpSrv == nil {
			return
		}
		err := cs.httpSrv.Serve(ln)
		if err != nil {
			cs.logger.Fatal(err)
		}
		// sending the quit signal.
		cs.quitChan <- struct{}{}
	}()

	go cs.readServiceEntries()

	cs.lookupServices()

	// Start the query loop
	go cs.queryLoop()
}

func (cs *cacheServer) lookupServices() {
	// Start the lookup
	err := mdns.Lookup("stream_publisher._tcp", cs.entriesChan)
	if err != nil {
		cs.logger.Fatal(err)
	}
}

func (cs *cacheServer) readServiceEntries() {
	for {
		select {
		case entry := <-cs.entriesChan:
			// Channel is closed, so exit
			if entry == nil {
				return
			}
			streamMetadata := strings.Split(entry.Info, "_")
			if len(streamMetadata) < 2 {
				cs.logger.Println("malformed stream publisher info: ", entry.Info)
				continue
			}
			prefix := streamMetadata[1]
			originAddr := entry.AddrV4.String() + ":" + strconv.Itoa(entry.Port)
			cs.originMapMu.Lock()
			cs.originMap[prefix] = originAddr
			cs.originMapMu.Unlock()
			cs.logger.Println("prefix- ", prefix)
			cs.logger.Println("originAddr- ", originAddr)
		}
	}
}

func (cs *cacheServer) queryLoop() {
	ticker := time.NewTicker(cs.queryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-cs.queryDoneChan:
			cs.logger.Println("Exiting from query loop")
			cs.quitChan <- struct{}{}
			return
		case <-ticker.C:
			cs.logger.Println("Refreshing service list")
			cs.lookupServices()
		}
	}
}

// Close gracefully shuts down the cache server.
func (cs *cacheServer) Close() {
	err := cs.cache.Close()
	if err != nil {
		cs.logger.Println(err)
	}

	err = cs.httpSrv.Shutdown()
	if err != nil {
		cs.logger.Println(err)
	}

	// Wait for the http server to finish.
	<-cs.quitChan
	cs.logger.Println("Done with http server")

	// Exiting the query loop.
	cs.queryDoneChan <- struct{}{}
	<-cs.quitChan

	// Close the entries chan.
	cs.logger.Println("Closing the entries chan")
	close(cs.entriesChan)
}

func createCache(logger *log.Logger) ybc.Cacher {
	config := ybc.Config{
		MaxItemsCount: ybc.SizeT(*maxItemsCount),
		DataFileSize:  ybc.SizeT(*cacheSize) * ybc.SizeT(1024*1024),
	}

	var err error
	var cache ybc.Cacher

	cacheFilesPath_ := strings.Split(*cacheFilesPath, ",")
	cacheFilesCount := len(cacheFilesPath_)
	logger.Println("Opening data files. This can take a while for the first time if files are big")
	if cacheFilesCount < 2 {
		if cacheFilesPath_[0] != "" {
			config.DataFile = cacheFilesPath_[0] + ".data"
			config.IndexFile = cacheFilesPath_[0] + ".index"
		}
		cache, err = config.OpenCache(true)
		if err != nil {
			logger.Fatalf("Cannot open cache: [%s]", err)
		}
	} else if cacheFilesCount > 1 {
		config.MaxItemsCount /= ybc.SizeT(cacheFilesCount)
		config.DataFileSize /= ybc.SizeT(cacheFilesCount)
		var configs ybc.ClusterConfig
		configs = make([]*ybc.Config, cacheFilesCount)
		for i := 0; i < cacheFilesCount; i++ {
			cfg := config
			cfg.DataFile = cacheFilesPath_[i] + ".cdn-booster.data"
			cfg.IndexFile = cacheFilesPath_[i] + ".cdn-booster.index"
			configs[i] = &cfg
		}
		cache, err = configs.OpenCluster(true)
		if err != nil {
			logger.Fatalf("Cannot open cache cluster: [%s]", err)
		}
	}
	logger.Println("Data files have been opened")
	return cache
}

func (cs *cacheServer) serveHttp(addr string) (*fasthttp.Server, net.Listener) {
	if addr == "" {
		return nil, nil
	}
	ln := cs.listen(addr)
	cs.logger.Printf("Listening http on [%s]", addr)
	return cs.serve(ln), ln
}

func (cs *cacheServer) listen(addr string) net.Listener {
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		cs.logger.Fatalf("Cannot listen [%s]: [%s]", addr, err)
	}
	return ln
}

func (cs *cacheServer) serve(ln net.Listener) *fasthttp.Server {
	s := &fasthttp.Server{
		Handler:      cs.requestHandler,
		Name:         "proxy-cache",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	return s
}

func (cs *cacheServer) requestHandler(ctx *fasthttp.RequestCtx) {
	h := &ctx.Request.Header
	if !ctx.IsGet() {
		ctx.Error("Method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	path := string(ctx.Path())
	if path == *statsRequestPath {
		var w bytes.Buffer
		cs.stats.WriteToStream(&w)
		ctx.Success("text/plain", w.Bytes())
		return
	}

	// Get the origin
	items := strings.Split(path, "_")
	if len(items) < 2 {
		cs.logger.Println("malformed url: ", path)
		ctx.Error("bad url", fasthttp.StatusInternalServerError)
		return
	}
	originPrefix := strings.TrimPrefix(items[0], "/")
	cs.logger.Println("originPrefix- ", originPrefix)
	// Avoid caching of m3u8 playlist file
	if strings.HasSuffix(path, ".m3u8") {
		cs.originMapMu.Lock()
		upstreamURL := *upstreamProtocol + "://" + cs.originMap[originPrefix] + path
		cs.originMapMu.Unlock()
		var req fasthttp.Request
		req.SetRequestURI(upstreamURL)

		var resp fasthttp.Response
		err := cs.upstreamClient.Do(&req, &resp)
		if err != nil {
			cs.logRequestError(h, "Cannot make request for [%s]: [%s]", upstreamURL, err)
			return
		}

		if resp.StatusCode() != fasthttp.StatusOK {
			cs.logRequestError(h, "Unexpected status code=%d for the response [%s]", resp.StatusCode(), ctx.RequestURI())
			return
		}

		contentType := string(resp.Header.ContentType())
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		buf := resp.Body()

		rh := &ctx.Response.Header
		rh.Set("Access-Control-Allow-Origin", "*")
		rh.Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		ctx.Success(contentType, buf)
		return
	}

	if len(h.Peek("If-None-Match")) > 0 {
		resp := &ctx.Response
		resp.SetStatusCode(fasthttp.StatusNotModified)
		resp.Header.Set("Etag", "W/\"CacheForever\"")
		atomic.AddInt64(&cs.stats.IfNoneMatchHitsCount, 1)
		return
	}

	v := cs.keyPool.Get()
	if v == nil {
		v = make([]byte, 128)
	}
	key := v.([]byte)
	cs.originMapMu.Lock()
	key = append(key[:0], []byte(cs.originMap[originPrefix])...)
	cs.originMapMu.Unlock()
	key = append(key, ctx.Path()...)
	item, err := cs.cache.GetDeItem(key, time.Second)
	if err != nil {
		if err != ybc.ErrCacheMiss {
			cs.logger.Fatalf("Unexpected error when obtaining cache value by key=[%s]: [%s]", key, err)
		}

		atomic.AddInt64(&cs.stats.CacheMissesCount, 1)
		item = cs.fetchFromUpstream(h, key)
		if item == nil {
			ctx.Error("Service unavailable", fasthttp.StatusServiceUnavailable)
			return
		}
	} else {
		atomic.AddInt64(&cs.stats.CacheHitsCount, 1)
	}
	defer item.Close()
	cs.keyPool.Put(v)

	contentType, err := cs.loadContentType(h, item)
	if err != nil {
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	rh := &ctx.Response.Header
	rh.Set("Etag", "W/\"CacheForever\"")
	rh.Set("Cache-Control", "public, max-age=3600")
	rh.Set("Access-Control-Allow-Origin", "*")
	rh.Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")

	buf := item.Value()
	buf = buf[len(buf)-item.Available():]
	ctx.Success(contentType, buf)
	atomic.AddInt64(&cs.stats.BytesSentToClients, int64(len(buf)))
}

func (cs *cacheServer) fetchFromUpstream(h *fasthttp.RequestHeader, key []byte) *ybc.Item {
	upstreamURL := *upstreamProtocol + "://" + string(key)
	var req fasthttp.Request
	req.SetRequestURI(upstreamURL)

	var resp fasthttp.Response
	err := cs.upstreamClient.Do(&req, &resp)
	if err != nil {
		cs.logRequestError(h, "Cannot make request for [%s]: [%s]", key, err)
		return nil
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		cs.logRequestError(h, "Unexpected status code=%d for the response [%s]", resp.StatusCode(), key)
		return nil
	}

	contentType := string(resp.Header.ContentType())
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	body := resp.Body()
	contentLength := len(body)
	itemSize := contentLength + len(contentType) + 1
	txn, err := cs.cache.NewSetTxn(key, itemSize, ybc.MaxTtl)
	if err != nil {
		cs.logRequestError(h, "Cannot start set txn for response [%s], itemSize=%d: [%s]", key, itemSize, err)
		return nil
	}

	if err = cs.storeContentType(h, txn, contentType); err != nil {
		txn.Rollback()
		return nil
	}

	n, err := txn.Write(body)
	if err != nil {
		cs.logRequestError(h, "Cannot read response [%s] body with size=%d to cache: [%s]", key, contentLength, err)
		txn.Rollback()
		return nil
	}
	if n != contentLength {
		cs.logRequestError(h, "Unexpected number of bytes copied=%d from response [%s] to cache. Expected %d", n, key, contentLength)
		txn.Rollback()
		return nil
	}
	item, err := txn.CommitItem()
	if err != nil {
		cs.logRequestError(h, "Cannot commit set txn for response [%s], size=%d: [%s]", key, contentLength, err)
		return nil
	}
	atomic.AddInt64(&cs.stats.BytesReadFromUpstream, int64(n))
	return item
}

func (cs *cacheServer) storeContentType(h *fasthttp.RequestHeader, w io.Writer, contentType string) (err error) {
	strBuf := []byte(contentType)
	strSize := len(strBuf)
	if strSize > 255 {
		cs.logRequestError(h, "Too long content-type=[%s]. Its' length=%d should fit one byte", contentType, strSize)
		err = errors.New("Too long content-type")
		return
	}
	var sizeBuf [1]byte
	sizeBuf[0] = byte(strSize)
	if _, err = w.Write(sizeBuf[:]); err != nil {
		cs.logRequestError(h, "Cannot store content-type length in cache: [%s]", err)
		return
	}
	if _, err = w.Write(strBuf); err != nil {
		cs.logRequestError(h, "Cannot store content-type string with length=%d in cache: [%s]", strSize, err)
		return
	}
	return
}

func (cs *cacheServer) loadContentType(h *fasthttp.RequestHeader, r io.Reader) (contentType string, err error) {
	var sizeBuf [1]byte
	if _, err = r.Read(sizeBuf[:]); err != nil {
		cs.logRequestError(h, "Cannot read content-type length from cache: [%s]", err)
		return
	}
	strSize := int(sizeBuf[0])
	strBuf := make([]byte, strSize)
	if _, err = r.Read(strBuf); err != nil {
		cs.logRequestError(h, "Cannot read content-type string with length=%d from cache: [%s]", strSize, err)
		return
	}
	contentType = string(strBuf)
	return
}

func (cs *cacheServer) getRequestHost(h *fasthttp.RequestHeader) []byte {
	if *useClientRequestHost {
		return h.Host()
	}
	return cs.upstreamHostBytes
}

func (cs *cacheServer) logRequestError(h *fasthttp.RequestHeader, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	cs.logger.Printf("%s - %s - %s. %s", h.RequestURI(), h.Referer(), h.UserAgent(), msg)
}

type Stats struct {
	CacheHitsCount        int64
	CacheMissesCount      int64
	IfNoneMatchHitsCount  int64
	BytesReadFromUpstream int64
	BytesSentToClients    int64
}

func (s *Stats) WriteToStream(w io.Writer) {
	fmt.Fprintf(w, "Command-line flags\n")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Fprintf(w, "%s=%v\n", f.Name, f.Value)
	})
	fmt.Fprintf(w, "\n")

	requestsCount := s.CacheHitsCount + s.CacheMissesCount + s.IfNoneMatchHitsCount
	var cacheHitRatio float64
	if requestsCount > 0 {
		cacheHitRatio = float64(s.CacheHitsCount+s.IfNoneMatchHitsCount) / float64(requestsCount) * 100.0
	}
	fmt.Fprintf(w, "Requests count: %d\n", requestsCount)
	fmt.Fprintf(w, "Cache hit ratio: %.3f%%\n", cacheHitRatio)
	fmt.Fprintf(w, "Cache hits: %d\n", s.CacheHitsCount)
	fmt.Fprintf(w, "Cache misses: %d\n", s.CacheMissesCount)
	fmt.Fprintf(w, "If-None-Match hits: %d\n", s.IfNoneMatchHitsCount)
	fmt.Fprintf(w, "Read from upstream: %.3f MBytes\n", float64(s.BytesReadFromUpstream)/1000000)
	fmt.Fprintf(w, "Sent to clients: %.3f MBytes\n", float64(s.BytesSentToClients)/1000000)
	fmt.Fprintf(w, "Upstream traffic saved: %.3f MBytes\n", float64(s.BytesSentToClients-s.BytesReadFromUpstream)/1000000)
	fmt.Fprintf(w, "Upstream requests saved: %d\n", s.CacheHitsCount+s.IfNoneMatchHitsCount)
}
