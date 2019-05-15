// CDN booster
//
// This is a dumb HTTP proxy, which caches files obtained from upstreamHost.
// Derived from https://github.com/valyala/ybc/tree/master/apps/go/cdn-booster
//
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

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

func main() {
	iniflags.Parse()
	logger := log.New(os.Stdout, "[proxy-cache] ", log.LstdFlags|log.Lshortfile)

	cs := NewCacheServer(logger)
	cs.Start()

	// listen for signals
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	// Block until one of the signals above is received
	<-signalCh
	logger.Println("Quit signal received, initializing shutdown...")
	logger.Println("Stopping HTTP server")

	cs.Close()
}
