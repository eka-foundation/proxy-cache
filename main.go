// CDN booster
//
// This is a dumb HTTP proxy, which caches files obtained from upstreamHost.
// Derived from https://github.com/valyala/ybc/tree/master/apps/go/cdn-booster
//
package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/ybc/bindings/go/ybc"
	"github.com/vharitonsky/iniflags"
)

var (
	cacheFilesPath = flag.String("cacheFilesPath", "",
		"Path to cache file. Leave empty for anonymous non-persistent cache.\n"+
			"Enumerate multiple files delimited by comma for creating a cluster of caches.\n"+
			"This can increase performance only if frequently accessed items don't fit RAM\n"+
			"and each cache file is located on a distinct physical storage.")
	cacheSize            = flag.Int("cacheSize", 100, "The total cache size in Mbytes")
	httpsCertFile        = flag.String("httpsCertFile", "/etc/ssl/certs/ssl-cert-snakeoil.pem", "Path to HTTPS server certificate. Used only if listenHttpsAddr is set")
	httpsKeyFile         = flag.String("httpsKeyFile", "/etc/ssl/private/ssl-cert-snakeoil.key", "Path to HTTPS server key. Used only if listenHttpsAddr is set")
	httpsListenAddr      = flag.String("httpsListenAddr", "", "TCP address to listen to HTTPS requests. Leave empty if you don't need https")
	listenAddr           = flag.String("listenAddr", ":8098", "TCP address to listen to HTTP requests. Leave empty if you don't need http")
	maxIdleUpstreamConns = flag.Int("maxIdleUpstreamConns", 50, "The maximum idle connections to upstream host")
	maxItemsCount        = flag.Int("maxItemsCount", 100*1000, "The maximum number of items in the cache")
	statsRequestPath     = flag.String("statsRequestPath", "/static_proxy_stats", "Path to page with statistics")
	upstreamHost         = flag.String("upstreamHost", "www.google.com", "Upstream host to proxy data from. May include port in the form 'host:port'")
	upstreamProtocol     = flag.String("upstreamProtocol", "http", "Use this protocol when talking to the upstream")
	useClientRequestHost = flag.Bool("useClientRequestHost", false, "If set to true, then use 'Host' header from client requests in requests to upstream host. Otherwise use upstreamHost as a 'Host' header in upstream requests")
)

var (
	cache             ybc.Cacher
	stats             Stats
	upstreamClient    *fasthttp.HostClient
	logger            *log.Logger
	upstreamHostBytes []byte
)

func main() {
	iniflags.Parse()
	logger = log.New(os.Stdout, "[proxy-cache] ", log.LstdFlags|log.Lshortfile)
	upstreamHostBytes = []byte(*upstreamHost)

	cache = createCache()
	defer cache.Close()

	upstreamClient = &fasthttp.HostClient{
		Addr:     *upstreamHost,
		MaxConns: *maxIdleUpstreamConns,
	}

	httpsSrv, httpsLn := serveHttps(*httpsListenAddr)
	httpSrv, httpLn := serveHttp(*listenAddr)

	go func() {
		if httpsSrv == nil {
			return
		}
		err := httpsSrv.Serve(httpsLn)
		if err != nil {
			logger.Fatal(err)
		}
	}()

	go func() {
		if httpSrv == nil {
			return
		}
		err := httpSrv.Serve(httpLn)
		if err != nil {
			logger.Fatal(err)
		}
	}()

	// listen for signals
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	// Block until one of the signals above is received
	<-signalCh
	logger.Println("Quit signal received, initializing shutdown...")
	logger.Println("Stopping HTTP server")

	if httpSrv != nil {
		err := httpSrv.Shutdown()
		if err != nil {
			logger.Println(err)
		}
	}

	if httpsSrv != nil {
		err := httpsSrv.Shutdown()
		if err != nil {
			logger.Println(err)
		}
	}
}

func createCache() ybc.Cacher {
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

func serveHttps(addr string) (*fasthttp.Server, net.Listener) {
	if addr == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(*httpsCertFile, *httpsKeyFile)
	if err != nil {
		logger.Fatalf("Cannot load certificate: [%s]", err)
	}
	c := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	ln := tls.NewListener(listen(addr), c)
	logger.Printf("Listening https on [%s]", addr)
	return serve(ln), ln
}

func serveHttp(addr string) (*fasthttp.Server, net.Listener) {
	if addr == "" {
		return nil, nil
	}
	ln := listen(addr)
	logger.Printf("Listening http on [%s]", addr)
	return serve(ln), ln
}

func listen(addr string) net.Listener {
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		logger.Fatalf("Cannot listen [%s]: [%s]", addr, err)
	}
	return ln
}

func serve(ln net.Listener) *fasthttp.Server {
	s := &fasthttp.Server{
		Handler:      requestHandler,
		Name:         "proxy-cache",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	return s
}

var keyPool sync.Pool

func requestHandler(ctx *fasthttp.RequestCtx) {
	h := &ctx.Request.Header
	if !ctx.IsGet() {
		ctx.Error("Method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	if string(ctx.RequestURI()) == *statsRequestPath {
		var w bytes.Buffer
		stats.WriteToStream(&w)
		ctx.Success("text/plain", w.Bytes())
		return
	}

	// Avoid caching of m3u8 playlist file
	if strings.HasSuffix(string(ctx.RequestURI()), ".m3u8") {
		upstreamUrl := fmt.Sprintf("%s://%s%s", *upstreamProtocol, *upstreamHost, h.RequestURI())
		var req fasthttp.Request
		req.SetRequestURI(upstreamUrl)

		var resp fasthttp.Response
		err := upstreamClient.Do(&req, &resp)
		if err != nil {
			logRequestError(h, "Cannot make request for [%s]: [%s]", h.RequestURI(), err)
			return
		}

		if resp.StatusCode() != fasthttp.StatusOK {
			logRequestError(h, "Unexpected status code=%d for the response [%s]", resp.StatusCode(), h.RequestURI())
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
		atomic.AddInt64(&stats.IfNoneMatchHitsCount, 1)
		return
	}

	v := keyPool.Get()
	if v == nil {
		v = make([]byte, 128)
	}
	key := v.([]byte)
	key = append(key[:0], getRequestHost(h)...)
	key = append(key, ctx.RequestURI()...)
	item, err := cache.GetDeItem(key, time.Second)
	if err != nil {
		if err != ybc.ErrCacheMiss {
			logger.Fatalf("Unexpected error when obtaining cache value by key=[%s]: [%s]", key, err)
		}

		atomic.AddInt64(&stats.CacheMissesCount, 1)
		item = fetchFromUpstream(h, key)
		if item == nil {
			ctx.Error("Service unavailable", fasthttp.StatusServiceUnavailable)
			return
		}
	} else {
		atomic.AddInt64(&stats.CacheHitsCount, 1)
	}
	defer item.Close()
	keyPool.Put(v)

	contentType, err := loadContentType(h, item)
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
	atomic.AddInt64(&stats.BytesSentToClients, int64(len(buf)))
}

func fetchFromUpstream(h *fasthttp.RequestHeader, key []byte) *ybc.Item {
	upstreamUrl := fmt.Sprintf("%s://%s%s", *upstreamProtocol, *upstreamHost, h.RequestURI())
	var req fasthttp.Request
	req.SetRequestURI(upstreamUrl)

	var resp fasthttp.Response
	err := upstreamClient.Do(&req, &resp)
	if err != nil {
		logRequestError(h, "Cannot make request for [%s]: [%s]", key, err)
		return nil
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		logRequestError(h, "Unexpected status code=%d for the response [%s]", resp.StatusCode(), key)
		return nil
	}

	contentType := string(resp.Header.ContentType())
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	body := resp.Body()
	contentLength := len(body)
	itemSize := contentLength + len(contentType) + 1
	txn, err := cache.NewSetTxn(key, itemSize, ybc.MaxTtl)
	if err != nil {
		logRequestError(h, "Cannot start set txn for response [%s], itemSize=%d: [%s]", key, itemSize, err)
		return nil
	}

	if err = storeContentType(h, txn, contentType); err != nil {
		txn.Rollback()
		return nil
	}

	n, err := txn.Write(body)
	if err != nil {
		logRequestError(h, "Cannot read response [%s] body with size=%d to cache: [%s]", key, contentLength, err)
		txn.Rollback()
		return nil
	}
	if n != contentLength {
		logRequestError(h, "Unexpected number of bytes copied=%d from response [%s] to cache. Expected %d", n, key, contentLength)
		txn.Rollback()
		return nil
	}
	item, err := txn.CommitItem()
	if err != nil {
		logRequestError(h, "Cannot commit set txn for response [%s], size=%d: [%s]", key, contentLength, err)
		return nil
	}
	atomic.AddInt64(&stats.BytesReadFromUpstream, int64(n))
	return item
}

func storeContentType(h *fasthttp.RequestHeader, w io.Writer, contentType string) (err error) {
	strBuf := []byte(contentType)
	strSize := len(strBuf)
	if strSize > 255 {
		logRequestError(h, "Too long content-type=[%s]. Its' length=%d should fit one byte", contentType, strSize)
		err = fmt.Errorf("Too long content-type")
		return
	}
	var sizeBuf [1]byte
	sizeBuf[0] = byte(strSize)
	if _, err = w.Write(sizeBuf[:]); err != nil {
		logRequestError(h, "Cannot store content-type length in cache: [%s]", err)
		return
	}
	if _, err = w.Write(strBuf); err != nil {
		logRequestError(h, "Cannot store content-type string with length=%d in cache: [%s]", strSize, err)
		return
	}
	return
}

func loadContentType(h *fasthttp.RequestHeader, r io.Reader) (contentType string, err error) {
	var sizeBuf [1]byte
	if _, err = r.Read(sizeBuf[:]); err != nil {
		logRequestError(h, "Cannot read content-type length from cache: [%s]", err)
		return
	}
	strSize := int(sizeBuf[0])
	strBuf := make([]byte, strSize)
	if _, err = r.Read(strBuf); err != nil {
		logRequestError(h, "Cannot read content-type string with length=%d from cache: [%s]", strSize, err)
		return
	}
	contentType = string(strBuf)
	return
}

func getRequestHost(h *fasthttp.RequestHeader) []byte {
	if *useClientRequestHost {
		return h.Host()
	}
	return upstreamHostBytes
}

func logRequestError(h *fasthttp.RequestHeader, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	logger.Printf("%s - %s - %s. %s", h.RequestURI(), h.Referer(), h.UserAgent(), msg)
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
