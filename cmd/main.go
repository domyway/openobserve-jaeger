package main

import (
	"flag"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"openobserve-jaeger/internal/config"
	"openobserve-jaeger/internal/transport/http"
)

var conf = flag.String("conf", "", "set your config file path. Example: ./configs/config.yaml")

func main() {
	flag.Parse()
	data, err := ioutil.ReadFile(*conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = yaml.Unmarshal(data, &config.Cfg)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	r := http.NewHTTPServer()
	// Listen and Server in 0.0.0.0:8080
	r.Run(":8080")
}
