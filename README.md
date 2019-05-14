CDN booster

This is a dumb HTTP proxy, which caches files obtained from upstreamHost.

Thanks to YBC it has the following features:
  * It is extremely fast. According to my artificial tests go-cdn-booster
    easily handles 2x-4x more requests per second comparing to nginx with
    enabled proxy cache (100K vs 25K).
  * Cached items survive go-cdn-booster restart if backed by cacheFilesPath.
  * Cache size isn't limited by RAM size.
  * Optimized for SSDs and HDDs.
  * Performance shouldn't depend on the number of cached items.
  * It has protection agains dogpile effect (aka "thundering herd").
  * It is deadly simple in configuration and maintenance. There is no need
    in running 'cleaners', 'watchdogs' or other similar tools. There is no
    need in setting up third-party libraries and/or tools. There is no
    need in writing complex configuration files. Just pass a couple of command
    line arguments to it and enjoy!

Thanks to Go it has the following features:
  * Scales automatically to multiple CPUs.
  * Handles large number of concurrent connections with minimum performance
    loss and minimum memory usage.
  * Supports keep-alive connections to upstream servers out-of-the-box.
  * Has easy-to-read-and-hack code.

Additional features:
  * Absolutely protected from slowloris DoS.
    See http://en.wikipedia.org/wiki/Slowloris for details.
  * Easily handles more than 100K requests per second.
  * Enforce a certain protocol (eg. HTTP/HTTPS) when talking to the upstream.
    Useful, for example, to serve content over HTTP but fetch it from the upstream
    over HTTPS. 

Currently go-cdn-booster has the following limitations:
  * Supports only GET requests.
  * Doesn't respect HTTP headers received from both the client and
    the upstream host.
  * Optimized for small static files aka images, js and css with sizes
    not exceeding few Mb each.
  * It caches all files without expiration time.
    Actually this is a feature :)
  * It caches only responses with 200 status codes.
  * It caches only responses with excplicitly set Content-Length.

Use cases:
  * Substitution for Nginx, Varnish, etc. in front of large and/or slow
    static file servers.
  * Poor-man's DIY geographically distributed CDN.

------------------------
How to build and run it?

$ sudo apt-get install golang
$ go get -u github.com/valyala/ybc/apps/go/cdn-booster
$ go build -tags release github.com/valyala/ybc/apps/go/cdn-booster
$ ./cdn-booster -help
