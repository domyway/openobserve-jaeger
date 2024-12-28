# OpenObserve-Jaeger
when we use jaeger-ui as front-ui, openobserve as trace backend,
we need to use openobserve-jaeger as a middleware to convert datastruct to jaeger-ui .
and only support 4 api right now:

```shell
"/api/traces",
"/api/traces/:id",
"/api/services/:servicename/operations",
"/api/services",
```

# setup

## step1
config your own `openobserve` server address and auth token in `config.yaml` file.
```yaml
openobserve:
  addr: xxxx # the router of ip:port or domain
  auth: cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM= # openobserve auth
  default_trace_detail_search_range_time: 24 # unit: hour  ps: search max time range in traceid detail page, openobserve must provide start_time and end_time
  default_queryui_max_search_range_time: 24 # unit: hour    ps: jeager-ui search form support max range hour
  default_servicename_size: 1000 # /api/services max service list count
  default_operationname_size: 10000 # /api/operations service operation list count
  default_span_size: 10000 # /api/traces max span list count
```

## step2 
config your own `docker-compose.yaml` and `nginx.conf` file.
make sure everything is ok, then run the following command.

```shell
cd deploy/docker
docker-compose up -d
```

## step3

```shell
go build -o openobserve-jaeger cmd/main.go 
./openobserve-jaeger -conf configs/config.yaml 
```

## step4 
open browser and visit `http://localhost:16687/`