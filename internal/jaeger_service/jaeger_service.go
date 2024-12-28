package jaeger_service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/model/adjuster"
	uiconv "github.com/jaegertracing/jaeger/model/converter/json"
	ui "github.com/jaegertracing/jaeger/model/json"
	"github.com/jaegertracing/jaeger/pkg/multierror"
	"github.com/jaegertracing/jaeger/plugin/storage/es/spanstore/dbmodel"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/trace"
	"log"
	"net/http"
	"openobserve-jaeger/internal/config"
	"openobserve-jaeger/internal/errors"
	"openobserve-jaeger/internal/openobserve_service"
	"regexp"
	"strings"
	"sync"
	"time"
)

// TraceQueryParameters contains parameters of a trace query.
type TraceQueryParameters struct {
	ServiceName   []string
	OperationName []string
	Tags          map[string]string
	StartTimeMin  time.Time
	StartTimeMax  time.Time
	DurationMin   time.Duration
	DurationMax   time.Duration
	NumTraces     int
	Version       string
	SkipWal       bool
	SearchType    string
}

type DbmodelSpanFixedKey struct {
	ServiceName            string
	StartTime              string
	EndTime                string
	Timestamp              string
	TraceID                string
	SpanID                 string
	Duration               string
	Flags                  string
	OperationName          string
	SpanKind               string
	SpanStatus             string
	Error                  string
	ReferenceParentSpanId  string
	ReferenceParentTraceId string
	ReferenceRefType       string
	Events                 string
}

var (
	OOSpanFixedKey = DbmodelSpanFixedKey{
		ServiceName:            "service_name",
		StartTime:              "start_time",
		EndTime:                "end_time",
		Timestamp:              "_timestamp",
		TraceID:                "trace_id",
		SpanID:                 "span_id",
		Duration:               "duration",
		Flags:                  "flags",
		OperationName:          "operation_name",
		SpanKind:               "span_kind",
		SpanStatus:             "span_status",
		Error:                  "error",
		ReferenceParentSpanId:  "reference_parent_span_id",
		ReferenceParentTraceId: "reference_parent_trace_id",
		ReferenceRefType:       "reference_ref_type",
		Events:                 "events",
	}

	// 所有不是ProcessTags的都转换为Tags
	DbModelProcessTagsRulesReg = regexp.MustCompile("" +
		"")
	ServiceCacheKey                  = "jaegerServiceName"
	OperationNameCacheKey            = "jaegerOperationName"
	MaxListSearchListTimeRange int64 = 3600000 // 1 hour
)

type JaegerService struct {
	ooservice  *openobserve_service.OpenObserveService
	adjuster   adjuster.Adjuster
	once       sync.Once
	httpclient *resty.Client
}

type JaegerStructuredResponse struct {
	Data   interface{}             `json:"data"`
	Total  int                     `json:"total"`
	Limit  int                     `json:"limit"`
	Offset int                     `json:"offset"`
	Errors []JaegerStructuredError `json:"errors"`
}

func (j JaegerStructuredResponse) StatusCode() int {
	code := http.StatusOK
	if len(j.Errors) > 0 {
		code = j.Errors[0].Code
	}

	return code
}

type JaegerStructuredError struct {
	Code    int        `json:"code,omitempty"`
	Msg     string     `json:"msg"`
	TraceID ui.TraceID `json:"traceID,omitempty"`
}

const (
	TraceAPI    = "TraceAPI"
	MetadataAPI = "MetadataAPI"
)

func NewJaegerService() *JaegerService {
	return &JaegerService{
		ooservice:  openobserve_service.NewOpenObserveService(),
		adjuster:   adjuster.Sequence(StandardAdjusters(time.Second)...),
		httpclient: resty.New(),
	}
}

func StandardAdjusters(maxClockSkewAdjust time.Duration) []adjuster.Adjuster {
	return []adjuster.Adjuster{
		adjuster.SpanIDDeduper(),
		adjuster.IPTagAdjuster(),
		adjuster.SortLogFields(),
		adjuster.SpanReferences(),
	}
}

func (s *JaegerService) ooValuesApiToJaegerRespData(data *openobserve_service.OpenObserveResp) ([]interface{}, int) {
	res := make([]interface{}, 0, 1000)

	if data.Total <= 0 {
		return res, 0
	}

	if len(data.Hits) > 0 {
		values := data.Hits[0]
		if v, ok := values["values"]; ok {
			vm := cast.ToSlice(v)
			for _, vv := range vm {
				vvv := cast.ToStringMap(vv)
				res = append(res, vvv["zo_sql_key"])
			}
		}
	}

	return res, len(res)
}

func (s *JaegerService) ooFieldValueApiToJaegerRespData(data *openobserve_service.OpenObserveResp, key string) ([]interface{}, int) {
	res := make([]interface{}, 0, 1000)

	if data.Total <= 0 {
		return res, 0
	}

	if len(data.Hits) > 0 {
		for _, hit := range data.Hits {
			if v, ok := hit[key]; ok {
				res = append(res, v)
			}
		}
	}

	return res, len(res)
}

func (s *JaegerService) GetService(ctx *gin.Context, q *openobserve_service.OOQuery) JaegerStructuredResponse {
	return s.getService(ctx, q)
}

func (s *JaegerService) getService(ctx *gin.Context, q *openobserve_service.OOQuery) JaegerStructuredResponse {
	jaegerResp := JaegerStructuredResponse{
		Errors: make([]JaegerStructuredError, 0),
	}

	ooresp, err := s.ooservice.GetService(ctx)
	if err != nil {
		if e, ok := err.(*errors.Error); ok {
			jaegerResp.Errors = append(jaegerResp.Errors, JaegerStructuredError{
				Code: int(e.GetCode()),
				Msg:  e.GetMessage(),
			})
		} else {
			jaegerResp.Errors = append(jaegerResp.Errors, JaegerStructuredError{
				Code: int(500),
				Msg:  err.Error(),
			})
		}

		return jaegerResp
	}

	jaegerResp.Data, jaegerResp.Total = s.ooFieldValueApiToJaegerRespData(ooresp, "service_name")
	return jaegerResp
}

func (s *JaegerService) GetOperations(ctx *gin.Context, q *openobserve_service.OOQuery) JaegerStructuredResponse {
	jaegerResp := JaegerStructuredResponse{
		Errors: make([]JaegerStructuredError, 0),
	}

	ooresp, err := s.ooservice.GetServiceOperation(ctx, q.ServiceName, q.SearchType)
	if err != nil {
		if e, ok := err.(*errors.Error); ok {
			jaegerResp.Errors = append(jaegerResp.Errors, JaegerStructuredError{
				Code: int(e.GetCode()),
				Msg:  e.GetMessage(),
			})
		} else {
			jaegerResp.Errors = append(jaegerResp.Errors, JaegerStructuredError{
				Code: int(500),
				Msg:  err.Error(),
			})
		}

		return jaegerResp
	}

	jaegerResp.Data, jaegerResp.Total = s.ooFieldValueApiToJaegerRespData(ooresp, "operation_name")
	return jaegerResp
}

func (s *JaegerService) FindTraces(ctx *gin.Context, q *TraceQueryParameters) JaegerStructuredResponse {
	jaegerResp := JaegerStructuredResponse{
		Data:   make([]string, 0),
		Errors: make([]JaegerStructuredError, 0),
	}

	// uiErrors := make([]JaegerStructuredError, 0)
	traceIds, structErrors := s.findTracesIds(ctx, q)
	if len(structErrors) > 0 {
		if structErrors[0].Code == 404 {
			return jaegerResp
		} else {
			jaegerResp.Errors = structErrors
			return jaegerResp
		}

	}

	// todo: search all the time for the whole traceid
	// use default_queryui_max_search_range_time for performence temporary
	// rangeTime, _ := config.Get("openobserve.default_queryui_max_search_range_time").Int()
	spanSize := config.Cfg.OpenObserve.DefaultSpanSize
	qq := &TraceQueryParameters{
		StartTimeMin: q.StartTimeMin,
		StartTimeMax: q.StartTimeMax,
		NumTraces:    int(spanSize),
		SearchType:   openobserve_service.UiSearchType,
	}

	uiTraces := make([]*ui.Trace, int(spanSize))
	uiTraces, structErrors = s.findTracesByIds(ctx, qq, traceIds)

	if len(structErrors) > 0 {
		if structErrors[0].Code == 404 {
			return jaegerResp
		} else {
			jaegerResp.Errors = structErrors
			return jaegerResp
		}
	}

	jaegerResp.Data = uiTraces
	jaegerResp.Total = len(uiTraces)

	return jaegerResp
}

func (s *JaegerService) findTracesIds(ctx *gin.Context, q *TraceQueryParameters) ([]string, []JaegerStructuredError) {
	sql, stream_api := s.buildSQL(ctx, "trace_id, MIN(_timestamp) AS _timestamp", q, openobserve_service.SearchTraceListStream)
	log.Printf("findTracesIds sql: %s", sql)

	qq := openobserve_service.OOSearchQuery{
		Query: openobserve_service.OOSearchQueryQuery{
			SqlMode:   "full",
			StartTime: q.StartTimeMin.UnixMicro(),
			EndTime:   q.StartTimeMax.UnixMicro(),
			Sql:       base64.StdEncoding.EncodeToString([]byte(sql)),
		},
	}

	if q.Version == "v3" {
		qq.Query.SkipWal = true
		qq.SearchType = openobserve_service.BackgroundSearchType
	}

	if q.Version == "v4" {
		qq.SearchType = openobserve_service.BackgroundSearchType
	}

	var ooresp *openobserve_service.OpenObserveResp
	var err error
	if stream_api == TraceAPI {
		ooresp, err = s.ooservice.SearchTraces(ctx, qq)
	} else {
		ooresp, err = s.ooservice.SearchMeatadata(ctx, qq)
	}

	if err != nil {
		if e, ok := err.(*errors.Error); ok {
			return nil, []JaegerStructuredError{
				{
					Code: int(e.GetCode()),
					Msg:  e.GetMessage(),
				},
			}
		} else {
			return nil, []JaegerStructuredError{
				{
					Code: int(500),
					Msg:  err.Error(),
				},
			}
		}
	}

	if len(ooresp.Hits) == 0 {
		return nil, []JaegerStructuredError{
			{
				Code: 404,
				Msg:  "trace not found",
			},
		}
	}

	traceid := make([]string, 0, len(ooresp.Hits))
	for _, trace := range ooresp.Hits {
		if id, ok := trace["trace_id"]; ok {
			traceid = append(traceid, cast.ToString(id))
		}
	}

	return traceid, nil
}

func (s *JaegerService) findTracesByIds(ctx *gin.Context, q *TraceQueryParameters, traceids []string) ([]*ui.Trace, []JaegerStructuredError) {
	if len(traceids) <= 0 {
		return nil, nil
	}

	traceidsql := "trace_id IN('" + strings.Join(traceids, "','") + "')"
	sql := fmt.Sprintf("SELECT * FROM default WHERE %s ORDER BY start_time DESC", traceidsql)
	return s.searchTracesByIds(ctx, q, sql, traceids)
}

func (s *JaegerService) searchTracesByIds(ctx *gin.Context, q *TraceQueryParameters, sql string, traceids []string) ([]*ui.Trace, []JaegerStructuredError) {
	log.Printf("findTracesByIds sql: %s", sql)

	qq := openobserve_service.OOSearchQuery{
		Query: openobserve_service.OOSearchQueryQuery{
			SqlMode:   "full",
			StartTime: q.StartTimeMin.UnixMicro(),
			EndTime:   q.StartTimeMax.UnixMicro(),
			Sql:       base64.StdEncoding.EncodeToString([]byte(sql)),
			Size:      int64(q.NumTraces),
			SkipWal:   q.SkipWal,
		},
		SearchType: q.SearchType,
	}

	ooresp, err := s.ooservice.SearchTraces(ctx, qq)
	if err != nil {
		return nil, []JaegerStructuredError{
			{
				Code: 500,
				Msg:  err.Error(),
				// TraceID: ui.TraceID(q.TraceID),
			},
		}
	}

	if len(ooresp.Hits) == 0 {
		return nil, []JaegerStructuredError{
			{
				Code: 404,
				Msg:  "trace not found",
				// TraceID: ui.TraceID(q.TraceID),
			},
		}
	}

	// format to openobserve_service.OpenObserveResp
	splitOOResp := make(map[string]*openobserve_service.OpenObserveResp)
	for _, span := range ooresp.Hits {
		traceid := cast.ToString(span["trace_id"])
		if traceid != "" {
			if _, ok := splitOOResp[traceid]; ok {
				splitOOResp[traceid].Hits = append(splitOOResp[traceid].Hits, span)
			} else {
				splitOOResp[traceid] = &openobserve_service.OpenObserveResp{
					Hits: []map[string]interface{}{
						span,
					},
				}
			}
		}
	}

	// build ui trace slice
	res := make([]*ui.Trace, 0, len(traceids))
	structErrors := make([]JaegerStructuredError, 0, len(traceids))
	if len(splitOOResp) > 0 {
		for id, resp := range splitOOResp {
			traces, jaegerErr := s.transOOToJaegerUI(ctx, resp, id)
			if jaegerErr != nil {
				structErrors = append(structErrors, *jaegerErr)
			}
			res = append(res, traces)
		}
	}

	return res, structErrors
}

func (s *JaegerService) buildSQL(ctx *gin.Context, fileds string, q *TraceQueryParameters, stream string) (string, string) {
	var sql, stream_api string
	if len(stream) == 0 || len(q.Tags) > 0 || len(q.OperationName) > 0 || q.DurationMax > 0 || q.DurationMin > 0 {
		stream = openobserve_service.SearchTraceDefaultStream
		sql = "SELECT trace_id, MIN(start_time) AS _timestamp FROM " + stream
		stream_api = TraceAPI
	} else {
		sql = "SELECT " + fileds + " FROM " + stream
		stream_api = MetadataAPI
	}

	cond := s.buildSQLCond(ctx, q)

	if len(cond) > 0 {
		sql = sql + " WHERE " + strings.Join(cond, " AND ")
	}

	sql = sql + " GROUP BY trace_id ORDER BY _timestamp DESC "

	if q.NumTraces > 0 {
		sql = sql + fmt.Sprintf(" LIMIT %d", q.NumTraces)
	}

	return sql, stream_api
}

func (s *JaegerService) buildSQLCond(ctx *gin.Context, q *TraceQueryParameters) []string {
	cond := make([]string, 0, 10)

	if len(q.ServiceName) == 1 {
		cond = append(cond, "service_name ='"+q.ServiceName[0]+"'")
	} else if len(q.ServiceName) > 1 {
		cond = append(cond, "service_name IN('"+strings.Join(q.ServiceName, "','")+"')")
	}

	if len(q.OperationName) > 0 {
		cond = append(cond, "operation_name IN('"+strings.Join(q.OperationName, "','")+"')")
	}

	if q.DurationMin > 0 {
		cond = append(cond, fmt.Sprintf("duration >= %d", q.DurationMin.Microseconds()))
	}

	if q.DurationMax > 0 {
		cond = append(cond, fmt.Sprintf("duration <= %d", q.DurationMax.Microseconds()))
	}

	if len(q.Tags) > 0 {
		tags := make([]string, 0, len(q.Tags))
		for k, v := range q.Tags {
			if k == OOSpanFixedKey.Error {
				vv := cast.ToString(v)
				if vv == "true" {
					tags = append(tags, "span_status='ERROR'")
				}

			} else {
				tags = append(tags, fmt.Sprintf("%s='%s'", k, cast.ToString(v)))
			}

		}

		if len(tags) > 0 {
			cond = append(cond, "("+strings.Join(tags, " AND ")+")")
		}
	}

	return cond
}

func (s *JaegerService) GetTrace(ctx *gin.Context, q *openobserve_service.OOQuery) JaegerStructuredResponse {
	resp := JaegerStructuredResponse{
		Errors: make([]JaegerStructuredError, 0),
	}

	uiErrors := make([]JaegerStructuredError, 0)

	var sql string
	sql = fmt.Sprintf("SELECT * FROM default WHERE trace_id = '%s' ORDER BY start_time", q.TraceID)
	var start, end int64
	if q.StartTime.IsZero() && q.EndTime.IsZero() {
		start = time.Now().Add(-time.Hour * time.Duration(config.Cfg.OpenObserve.DefaultTraceDetailSearchRange)).UnixMicro()
		end = time.Now().UnixMicro()
	} else {
		start = q.StartTime.UnixMicro()
		end = q.EndTime.UnixMicro()
	}

	qq := openobserve_service.OOSearchQuery{
		Query: openobserve_service.OOSearchQueryQuery{
			SqlMode:   "full",
			StartTime: start,
			EndTime:   end,
			Sql:       base64.StdEncoding.EncodeToString([]byte(sql)),
			Size:      -1, // get all trace id
		},
	}

	ooresp, err := s.ooservice.SearchTraces(ctx, qq)
	if err != nil {
		resp.Errors = append(uiErrors, JaegerStructuredError{
			Code:    500,
			Msg:     err.Error(),
			TraceID: ui.TraceID(q.TraceID),
		})

		return resp
	}

	if len(ooresp.Hits) == 0 {
		resp.Errors = append(uiErrors, JaegerStructuredError{
			Code:    404,
			Msg:     "trace not found",
			TraceID: ui.TraceID(q.TraceID),
		})

		return resp
	}

	traces, jaegerErr := s.transOOToJaegerUI(ctx, ooresp, q.TraceID)
	data := []*ui.Trace{traces}
	resp.Data = data

	if jaegerErr != nil {
		resp.Errors = append(resp.Errors, *jaegerErr)
	}

	return resp
}

func (s *JaegerService) transOOToJaegerUI(ctx *gin.Context, oo *openobserve_service.OpenObserveResp, traceStrID string) (*ui.Trace, *JaegerStructuredError) {
	if oo == nil {
		return nil, nil
	}
	// traceID, err := model.TraceIDFromString(traceStrID)
	trace, err := s.transOOToJaegerModelTrace(ctx, oo)
	if err != nil {
		return nil, &JaegerStructuredError{
			Code:    400,
			Msg:     "400",
			TraceID: ui.TraceID(traceStrID),
		}
	}
	var errors []error
	trace, err = s.adjuster.Adjust(trace)
	if err != nil {
		errors = append(errors, err)
	}

	uiTrace := uiconv.FromDomain(trace)
	var uiError *JaegerStructuredError
	if err := multierror.Wrap(errors); err != nil {
		uiError = &JaegerStructuredError{
			Msg:     err.Error(),
			TraceID: uiTrace.TraceID,
		}
	}

	return uiTrace, uiError
}

func (s *JaegerService) transOOToJaegerModelTrace(ctx *gin.Context, oo *openobserve_service.OpenObserveResp) (*model.Trace, error) {
	if oo == nil {
		return nil, nil
	}

	spanConverter := NewToDomain("@")

	spans := make([]*model.Span, 0, len(oo.Hits))
	for _, oospan := range oo.Hits {
		jsonSpan := s.transOOSpanToDbModelSpan(ctx, oospan)

		if jsonSpan == nil {
			continue
		}

		span, err := spanConverter.SpanToDomain(jsonSpan)
		if err != nil {
			log.Printf("spanid: %s, spanConverter.SpanToDomain err : %v\n", jsonSpan.SpanID, err)
			continue
		}

		if span != nil {
			spans = append(spans, span)
		}

	}

	return &model.Trace{Spans: spans}, nil
}

func (s *JaegerService) transOOSpanToDbModelSpan(ctx *gin.Context, oo map[string]interface{}) *dbmodel.Span {
	if oo == nil {
		return nil
	}

	startTime := cast.ToInt64(oo[OOSpanFixedKey.StartTime])
	st := time.Unix(startTime/1e9, (startTime % 1e9))
	dbSpan := &dbmodel.Span{
		TraceID:       dbmodel.TraceID(cast.ToString(oo[OOSpanFixedKey.TraceID])),
		SpanID:        dbmodel.SpanID(cast.ToString(oo[OOSpanFixedKey.SpanID])),
		OperationName: cast.ToString(oo[OOSpanFixedKey.OperationName]),
		Process: dbmodel.Process{
			ServiceName: cast.ToString(oo[OOSpanFixedKey.ServiceName]),
			Tags:        make([]dbmodel.KeyValue, 0),
		},
		Flags:           cast.ToUint32(oo[OOSpanFixedKey.Flags]),
		ParentSpanID:    dbmodel.SpanID(cast.ToString(oo[OOSpanFixedKey.ReferenceParentSpanId])),
		StartTime:       cast.ToUint64(st.UnixMicro()),
		StartTimeMillis: cast.ToUint64(st.UnixMilli()),
		Duration:        cast.ToUint64(oo[OOSpanFixedKey.Duration]),
		Logs:            make([]dbmodel.Log, 0),
		Tags:            make([]dbmodel.KeyValue, 0),
		References:      make([]dbmodel.Reference, 0),
	}

	newoo := s.trimSpanFixedKey(oo)
	dbSpan.Logs = s.collectOOLogs(newoo)
	dbSpan.Tags = s.collectOOTags(newoo)
	dbSpan.Process.Tags = s.collectOOProcessTags(newoo)
	dbSpan.References = s.collectOOReferences(newoo)

	return dbSpan
}

func (s *JaegerService) collectOOReferences(oo map[string]interface{}) []dbmodel.Reference {
	ref := make([]dbmodel.Reference, 0)
	if len(cast.ToString(oo[OOSpanFixedKey.ReferenceParentSpanId])) == 0 {
		return ref
	}

	// default CHILD_OF
	ReferenceRefType := strings.ToUpper(cast.ToString(oo[OOSpanFixedKey.ReferenceRefType]))
	if ReferenceRefType == "CHILDOF" {
		ReferenceRefType = "CHILD_OF"
	} else if ReferenceRefType == "FOLLOWS_FROM" {
		ReferenceRefType = "FOLLOWS_FROM"
	} else {
		ReferenceRefType = "CHILD_OF"
	}

	r := dbmodel.Reference{
		RefType: dbmodel.ReferenceType(ReferenceRefType),
		TraceID: dbmodel.TraceID(cast.ToString(oo[OOSpanFixedKey.ReferenceParentTraceId])),
		SpanID:  dbmodel.SpanID(cast.ToString(oo[OOSpanFixedKey.ReferenceParentSpanId])),
	}

	ref = append(ref, r)

	return ref
}

func (s *JaegerService) collectOOLogs(oo map[string]interface{}) []dbmodel.Log {
	logs := make([]dbmodel.Log, 0)
	if len(oo) == 0 {
		return logs
	}

	if events, ok := oo[OOSpanFixedKey.Events]; ok {
		evs := make([]map[string]interface{}, 1)
		err := json.Unmarshal([]byte(cast.ToString(events)), &evs)
		if err != nil {
			log.Printf("%#v", err)
			return logs
		}

		for _, v := range evs {
			log := dbmodel.Log{
				Timestamp: 0,
				Fields:    make([]dbmodel.KeyValue, 0),
			}

			startTime := cast.ToInt64(v[OOSpanFixedKey.Timestamp])
			st := time.Unix(startTime/1e9, (startTime % 1e9))
			log.Timestamp = cast.ToUint64(st.UnixMicro())
			for k, vvv := range v {
				if k == OOSpanFixedKey.Timestamp {
					continue
				}
				log.Fields = append(log.Fields, dbmodel.KeyValue{
					Key:   k,
					Type:  dbmodel.ValueType("string"),
					Value: cast.ToString(vvv),
				})
			}

			logs = append(logs, log)
		}

	}

	return logs
}

func (s *JaegerService) collectOOTags(oo map[string]interface{}) []dbmodel.KeyValue {
	kvs := make([]dbmodel.KeyValue, 0)
	if len(oo) == 0 {
		return kvs
	}

	for k, v := range oo {
		if k == OOSpanFixedKey.SpanKind {
			kind := cast.ToInt(v)
			value := ""
			switch trace.SpanKind(kind) {
			case trace.SpanKindUnspecified:
				value = "unspecified"
			case trace.SpanKindInternal:
				value = "internal"
			case trace.SpanKindServer:
				value = "server"
			case trace.SpanKindClient:
				value = "client"
			case trace.SpanKindProducer:
				value = "producer"
			case trace.SpanKindConsumer:
				value = "consumer"
			}

			kv := dbmodel.KeyValue{
				Key:   "span.kind",
				Type:  dbmodel.ValueType("string"),
				Value: value,
			}

			kvs = append(kvs, kv)
			continue
		}

		if k == OOSpanFixedKey.SpanStatus {
			value := cast.ToString(v)
			kv := dbmodel.KeyValue{
				Key:   "otel.status_code",
				Type:  dbmodel.ValueType("string"),
				Value: value,
			}

			kvs = append(kvs, kv)

			if value == "ERROR" {
				ekv := dbmodel.KeyValue{
					Key:   "error",
					Type:  dbmodel.ValueType("bool"),
					Value: "true",
				}

				kvs = append(kvs, ekv)
			}

			continue
		}

		if k == OOSpanFixedKey.Events {
			continue
		}

		if !DbModelProcessTagsRulesReg.MatchString(k) {
			kv := dbmodel.KeyValue{
				Key:   k,
				Type:  dbmodel.ValueType("string"),
				Value: v,
			}

			kvs = append(kvs, kv)
		}
	}

	return kvs
}

func (s *JaegerService) collectOOProcessTags(oo map[string]interface{}) []dbmodel.KeyValue {
	kvs := make([]dbmodel.KeyValue, 0)
	if len(oo) == 0 {
		return kvs
	}

	for k, v := range oo {
		if DbModelProcessTagsRulesReg.MatchString(k) {
			kv := dbmodel.KeyValue{
				Key:   k,
				Type:  dbmodel.ValueType("string"),
				Value: v,
			}

			kvs = append(kvs, kv)
		}
	}

	return kvs
}

func (s *JaegerService) trimSpanFixedKey(oo map[string]interface{}) map[string]interface{} {
	if len(oo) == 0 {
		return oo
	}
	newoo := make(map[string]interface{})
	for k, v := range oo {
		if k == OOSpanFixedKey.ServiceName ||
			k == OOSpanFixedKey.StartTime ||
			k == OOSpanFixedKey.EndTime ||
			k == OOSpanFixedKey.Timestamp ||
			k == OOSpanFixedKey.TraceID ||
			k == OOSpanFixedKey.SpanID ||
			k == OOSpanFixedKey.Duration ||
			k == OOSpanFixedKey.Flags ||
			k == OOSpanFixedKey.OperationName {
			continue
		}

		newoo[k] = v
	}

	return newoo
}
