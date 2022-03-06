package proxy

import (
	"fmt"
	"io"
	"net/http"

	log "github.com/sirupsen/logrus"
)

func ProxyTo(address string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		url := fmt.Sprintf("%s%s", address, req.URL.String())
		req, err := http.NewRequest(req.Method, url, req.Body)
		if err != nil {
			panic(err)
		}
		log.Error(url)
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer resp.Body.Close()
		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
