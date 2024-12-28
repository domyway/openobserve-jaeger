package openobserve_service

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/prometheus/common/model"
	"log"
	"net/http"
	"net/url"
	"openobserve-jaeger/internal/config"
	"openobserve-jaeger/internal/errors"
	"strconv"
	"strings"
	"time"
)

const (
	searchTraceAPI           = "/api/default/_search?type=traces"
	searchMetadataAPI        = "/api/default/_search?type=metadata"
	searchMetricstaAPI       = "/api/default/prometheus/api/v1/query_range"
	searchTraceValueAPI      = "/api/default/default/_values"
	searchEncoding           = "base64"
	SearchTraceDefaultStream = "default"
	SearchTraceListStream    = "trace_list_index"
	BackgroundSearchType     = "reports"
	UiSearchType             = "ui"
)

type OpenObserveService struct {
	client                   *resty.Client
	addr                     string
	traceindex_addr          []string
	auth                     string
	DefaultServicenameSize   int64
	DefaultOperationnameSize int64
	hashRing                 *hashRing
}

type OpenObserveResp struct {
	Took       int `json:"took"`
	TookDetail struct {
		Total            int `json:"total"`
		IdxTook          int `json:"idx_took"`
		WaitQueue        int `json:"wait_queue"`
		ClusterTotal     int `json:"cluster_total"`
		ClusterWaitQueue int `json:"cluster_wait_queue"`
	} `json:"took_detail"`
	Hits     []map[string]interface{} `json:"hits"`
	Total    int                      `json:"total"`
	From     int                      `json:"from"`
	Size     int                      `json:"size"`
	ScanSize int                      `json:"scan_size"`
	TraceId  string                   `json:"trace_id"`
}

type OpenobserveMetricsResp struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric struct {
				ServiceName string `json:"service_name"`
				StatusCode  string `json:"status_code"`
			} `json:"metric"`
			Values []model.Value `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

type OOQuery struct {
	TraceID       string `form:"trace_id"`
	ServiceName   string `form:"service_name"`
	ServiceTag    string `json:"service_tag" form:"service_tag"`
	StartTime     time.Time
	EndTime       time.Time
	StartTimeUnix int64  `json:"start_time" form:"start_time"`
	EndTimeUnix   int64  `json:"end_time" form:"end_time"`
	QuickSearch   bool   `json:"quicksearch" form:"quicksearch"`
	SearchType    string `json:"search_type" form:"search_type"`
}

type OOSearchQuery struct {
	Aggs       map[string]interface{} `json:"aggs"`
	Query      OOSearchQueryQuery     `json:"query"`
	Encoding   string                 `json:"encoding"`
	SearchType string                 `json:"search_type"`
}

type OOSearchQueryQuery struct {
	SqlMode   string `json:"sql_mode"`
	StartTime int64  `json:"start_time"`
	EndTime   int64  `json:"end_time"`
	From      int64  `json:"from"`
	Size      int64  `json:"size"`
	Sql       string `json:"sql"`
	SkipWal   bool   `json:"skip_wal"`
}

type OOMetricsPromQuery struct {
	StartTime int64  `json:"start"`
	EndTime   int64  `json:"end"`
	Query     string `json:"query"`
}

func (q OOMetricsPromQuery) ToQueryString() string {
	values := url.Values{}
	values.Add("start", strconv.FormatInt(q.StartTime, 10))
	values.Add("end", strconv.FormatInt(q.EndTime, 10))
	values.Add("query", q.Query)

	return values.Encode()
}

type OOValueQuery struct {
	Fields    string `json:"fields"`
	Size      int64  `json:"size"`
	StartTime int64  `json:"start_time"`
	EndTime   int64  `json:"end_time"`
	Type      string `json:"type"`
	Filter    string `json:"filter"`
}

type TraceIndexHttpResponse struct {
	// jhttp.ResponseBody
	Code    int32                 `json:"code"`
	Message string                `json:"msg"`
	Data    map[string]TraceIndex `json:"data"`
}

type TraceIndex struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type ServiceTagHttpResponse struct {
	// jhttp.ResponseBody
	Code    int32    `json:"code"`
	Message string   `json:"msg"`
	Data    []string `json:"data"`
}

type ServiceTagValueHttpResponse struct {
	// jhttp.ResponseBody
	Code    int32    `json:"code"`
	Message string   `json:"msg"`
	Data    []string `json:"data"`
}

type HttpClientOption struct {
	Method     string            `json:"method"`
	Api        string            `json:"api"`
	Query      string            `json:"query"`
	Body       interface{}       `json:"body"`
	Header     map[string]string `json:"header"`
	TimeOut    int               `json:"time_out"` // 单位ms
	Result     interface{}       `json:"result"`
	RetryTimes int               `json:"retry_times"` // 重试次数配置
}

func NewOpenObserveService() *OpenObserveService {
	return &OpenObserveService{
		client:                   resty.New(),
		addr:                     config.Cfg.OpenObserve.Addr,
		auth:                     config.Cfg.OpenObserve.Auth,
		DefaultServicenameSize:   config.Cfg.OpenObserve.DefaultServiceNameSize,
		DefaultOperationnameSize: config.Cfg.OpenObserve.DefaultOperationNameSize,
	}
}

func (oo *OpenObserveService) SearchTraces(ctx context.Context, q OOSearchQuery) (*OpenObserveResp, error) {
	return oo.Search(ctx, q, searchTraceAPI)
}

func (oo *OpenObserveService) SearchMeatadata(ctx context.Context, q OOSearchQuery) (*OpenObserveResp, error) {
	return oo.Search(ctx, q, searchMetadataAPI)
}

func (oo *OpenObserveService) Search(ctx context.Context, q OOSearchQuery, api string) (*OpenObserveResp, error) {
	var reqOpt HttpClientOption
	reqOpt.Header = map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Basic " + oo.auth,
	}
	reqOpt.Method = "POST"
	reqOpt.Api = api
	if len(q.Encoding) == 0 {
		q.Encoding = searchEncoding
	}
	if q.Aggs == nil {
		q.Aggs = make(map[string]interface{})
	}

	if q.SearchType == BackgroundSearchType {
		reqOpt.Query = "search_type=" + BackgroundSearchType
	} else if q.SearchType == "" {
		q.SearchType = UiSearchType
		reqOpt.Query = "search_type=" + UiSearchType
	}

	reqOpt.Body = q
	reqOpt.Result = OpenObserveResp{}

	oo.client.SetTimeout(time.Duration(reqOpt.TimeOut) * time.Second)
	r := oo.client.R().SetHeaders(reqOpt.Header).SetContext(ctx).SetQueryString(reqOpt.Query).SetBody(reqOpt.Body).SetResult(reqOpt.Result)
	r.Method = reqOpt.Method
	r.URL = strings.TrimRight(oo.addr+reqOpt.Api, "/")

	resp, err := r.Send()
	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, errors.New(int32(resp.StatusCode()), "status: "+resp.Status()+" Body: "+string(resp.Body()))
	}

	res := resp.Result()
	log.Printf("ooresp result: %#v", res)
	if ooresp, ok := res.(*OpenObserveResp); ok {
		log.Printf("ooresp result took total: %d ms, watiqueue: %d ms, session_id: %s, q: %v", ooresp.TookDetail.Total, ooresp.TookDetail.WaitQueue, ooresp.TraceId, q)
		// debug info
		if ooresp.TookDetail.Total > 4000 {
			log.Printf("ooresp slow result took total: %d ms, watiqueue: %d ms, session_id: %s, q: %v, api: %s", ooresp.TookDetail.Total, ooresp.TookDetail.WaitQueue, ooresp.TraceId, q, api)
		}
		return ooresp, nil
	}

	return nil, errors.New(int32(resp.StatusCode()), "Error Body: "+string(resp.Body()))
}

func (oo *OpenObserveService) GetService(ctx context.Context) (*OpenObserveResp, error) {
	sql := "SELECT service_name FROM distinct_values_traces_default GROUP BY service_name"
	qq := OOSearchQuery{
		Query: OOSearchQueryQuery{
			SqlMode:   "full",
			StartTime: time.Now().Add(-time.Hour * time.Duration(168)).UnixMicro(),
			EndTime:   time.Now().UnixMicro(),
			Sql:       base64.StdEncoding.EncodeToString([]byte(sql)),
			Size:      oo.DefaultServicenameSize,
		},
	}

	return oo.SearchMeatadata(ctx, qq)
}

func (oo *OpenObserveService) GetServiceOperation(ctx context.Context, service_name, search_type string) (*OpenObserveResp, error) {
	sql := "SELECT operation_name FROM distinct_values_traces_default " +
		"WHERE service_name = '" + service_name + "' GROUP BY operation_name"
	qq := OOSearchQuery{
		Query: OOSearchQueryQuery{
			SqlMode:   "full",
			StartTime: time.Now().Add(-time.Hour * time.Duration(168)).UnixMicro(),
			EndTime:   time.Now().UnixMicro(),
			Sql:       base64.StdEncoding.EncodeToString([]byte(sql)),
			Size:      oo.DefaultOperationnameSize,
		},
	}

	if len(search_type) > 0 {
		qq.SearchType = search_type
	}

	return oo.SearchMeatadata(ctx, qq)
}

func (oo *OpenObserveService) GetTraceServiceIndex(ctx context.Context, traceids []string, start, end int64) (*OpenObserveResp, error) {
	traceidsql := "trace_id IN('" + strings.Join(traceids, "','") + "')"
	relatetive_service_sql := fmt.Sprintf("SELECT service_name FROM \"trace_list_index\" where %s GROUP BY service_name", traceidsql)
	qq := OOSearchQuery{
		Query: OOSearchQueryQuery{
			SqlMode:   "full",
			StartTime: start,
			EndTime:   end,
			Sql:       base64.StdEncoding.EncodeToString([]byte(relatetive_service_sql)),
		},
	}

	return oo.SearchMeatadata(ctx, qq)
}
