package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"openobserve-jaeger/internal/jaeger_service"
	"strconv"
	"strings"
	"time"
)

const (
	defaultQueryLimit  = 20
	defaultLogDocLimit = 100

	traceIDParam     = "traceID"
	operationParam   = "operation"
	tagParam         = "tag"
	tagsParam        = "tags"
	startTimeParam   = "start"
	limitParam       = "limit"
	minDurationParam = "minDuration"
	maxDurationParam = "maxDuration"
	serviceParam     = "service"
	spanKindParam    = "spanKind"
	endTimeParam     = "end"
	prettyPrintParam = "prettyPrint"
	versionParam     = "version"
)

var (
	errMaxDurationGreaterThanMin        = fmt.Errorf("'%s' should be greater than '%s'", maxDurationParam, minDurationParam)
	errStartTimeGreaterThanStartTimeMax = errors.New("StartTime should not be greater than EndTime")
	// errServiceParameterRequired occurs when no service name is defined.
	errServiceParameterRequired = fmt.Errorf("parameter '%s' is required", serviceParam)
)

type (
	// queryParser handles the parsing of query parameters for traces.
	queryParser struct {
		queryLookbackDuration time.Duration
		timeNow               func() time.Time
	}

	traceQueryParameters struct {
		jaeger_service.TraceQueryParameters
		traceIDs []string
	}

	durationParser = func(s string) (time.Duration, error)
)

var qp = queryParser{
	queryLookbackDuration: 1 * time.Hour,
	timeNow:               time.Now,
}

func newDurationStringParser() durationParser {
	return func(s string) (time.Duration, error) {
		return time.ParseDuration(s)
	}
}

// parseTraceQueryParams takes a request and constructs a model of parameters.
//
// Why start/end parameters are expressed in microseconds:
//
//	Span searches operate on span latencies, which are expressed as microseconds in the data model, hence why
//	support for high accuracy in search query parameters is required.
//	Microsecond precision is a legacy artifact from zipkin origins where timestamps and durations
//	are in microseconds (see: https://zipkin.io/pages/instrumenting.html).
//
// Why duration parameters are expressed as duration strings like "1ms":
//
//	The search UI itself does not insist on exact units because it supports string like 1ms.
//	Go makes parsing duration strings like "1ms" very easy, hence why parsing of such strings is
//	deferred to the backend rather than Jaeger UI.
//
// Trace query syntax:
//
//	query ::= param | param '&' query
//	param ::= service | operation | limit | start | end | minDuration | maxDuration | tag | tags
//	service ::= 'service=' strValue
//	operation ::= 'operation=' strValue
//	limit ::= 'limit=' intValue
//	start ::= 'start=' intValue in unix microseconds
//	end ::= 'end=' intValue in unix microseconds
//	minDuration ::= 'minDuration=' strValue (units are "ns", "us" (or "µs"), "ms", "s", "m", "h")
//	maxDuration ::= 'maxDuration=' strValue (units are "ns", "us" (or "µs"), "ms", "s", "m", "h")
//	tag ::= 'tag=' key | 'tag=' keyvalue
//	key := strValue
//	keyValue := strValue ':' strValue
//	tags :== 'tags=' jsonMap
func (p *queryParser) parseTraceQueryParams(ctx *gin.Context, r *http.Request) (*traceQueryParameters, error) {
	service, _ := ctx.GetQueryArray(serviceParam)

	operation, _ := ctx.GetQueryArray(operationParam)

	startTime, err := p.parseTime(r, startTimeParam, time.Microsecond)
	if err != nil {
		return nil, err
	}
	endTime, err := p.parseTime(r, endTimeParam, time.Microsecond)
	if err != nil {
		return nil, err
	}

	tags, err := p.parseTags(r.Form[tagParam], r.Form[tagsParam])
	if err != nil {
		return nil, err
	}

	limitParam := r.FormValue(limitParam)
	limit := defaultQueryLimit
	if limitParam != "" {
		limitParsed, err := strconv.ParseInt(limitParam, 10, 32)
		if err != nil {
			return nil, err
		}
		limit = int(limitParsed)
	}

	parser := newDurationStringParser()
	minDuration, err := parseDuration(r, minDurationParam, parser, 0)
	if err != nil {
		return nil, err
	}

	maxDuration, err := parseDuration(r, maxDurationParam, parser, 0)
	if err != nil {
		return nil, err
	}

	var traceIDs []string

	for _, id := range ctx.Params {
		traceIDs = append(traceIDs, id.Value)
	}

	var version string
	version = r.FormValue(versionParam)

	traceQuery := &traceQueryParameters{
		TraceQueryParameters: jaeger_service.TraceQueryParameters{
			ServiceName:   service,
			OperationName: operation,
			StartTimeMin:  startTime,
			StartTimeMax:  endTime,
			Tags:          tags,
			NumTraces:     limit,
			DurationMin:   minDuration,
			DurationMax:   maxDuration,
			Version:       version,
		},
		traceIDs: traceIDs,
	}

	if err := p.validateTraceQuery(traceQuery); err != nil {
		return nil, err
	}
	return traceQuery, nil
}

func (p *queryParser) validateTraceQuery(traceQuery *traceQueryParameters) error {
	if len(traceQuery.traceIDs) == 0 && len(traceQuery.ServiceName) == 0 {
		return errServiceParameterRequired
	}
	if traceQuery.DurationMin != 0 && traceQuery.DurationMax != 0 {
		if traceQuery.DurationMax < traceQuery.DurationMin {
			return errMaxDurationGreaterThanMin
		}
	}

	if !traceQuery.StartTimeMin.IsZero() && !traceQuery.StartTimeMax.IsZero() {
		if traceQuery.StartTimeMax.Sub(traceQuery.StartTimeMin) <= 0 {
			return errStartTimeGreaterThanStartTimeMax
		}

		if traceQuery.StartTimeMax.Sub(traceQuery.StartTimeMin) > (time.Hour + 5*time.Minute) {
			return errors.New(fmt.Sprintf("time range should not be greater than 1 Hour"))
		}
	}

	return nil
}

func (p *queryParser) parseTags(simpleTags []string, jsonTags []string) (map[string]string, error) {
	retMe := make(map[string]string)
	for _, tag := range simpleTags {
		keyAndValue := strings.Split(tag, ":")
		if l := len(keyAndValue); l > 1 {
			retMe[keyAndValue[0]] = strings.Join(keyAndValue[1:], ":")
		} else {
			return nil, fmt.Errorf("malformed 'tag' parameter, expecting key:value, received: %s", tag)
		}
	}
	for _, tags := range jsonTags {
		var fromJSON map[string]string
		if err := json.Unmarshal([]byte(tags), &fromJSON); err != nil {
			return nil, fmt.Errorf("malformed 'tags' parameter, cannot unmarshal JSON: %w", err)
		}
		for k, v := range fromJSON {
			retMe[k] = v
		}
	}
	return retMe, nil
}

// parseTime parses the time parameter of an HTTP request that is represented the number of "units" since epoch.
// If the time parameter is empty, the current time will be returned.
func (p *queryParser) parseTime(r *http.Request, paramName string, units time.Duration) (time.Time, error) {
	formValue := r.FormValue(paramName)
	if formValue == "" {
		if paramName == startTimeParam {
			return p.timeNow().Add(-1 * p.queryLookbackDuration), nil
		}
		return p.timeNow(), nil
	}
	t, err := strconv.ParseInt(formValue, 10, 64)
	if err != nil {
		return time.Time{}, newParseError(err, paramName)
	}

	if t < 0 {
		return time.Time{}, newParseError(fmt.Errorf("negative time value"), paramName)
	}

	return time.Unix(0, 0).Add(time.Duration(t) * units), nil
}

// parseDuration parses the duration parameter of an HTTP request using the provided durationParser.
// If the duration parameter is empty, the given defaultDuration will be returned.
func parseDuration(r *http.Request, paramName string, parse durationParser, defaultDuration time.Duration) (time.Duration, error) {
	formValue := r.FormValue(paramName)
	if formValue == "" {
		return defaultDuration, nil
	}
	d, err := parse(formValue)
	if err != nil {
		return 0, newParseError(err, paramName)
	}
	return d, nil
}

func parseBool(r *http.Request, paramName string) (b bool, err error) {
	formVal := r.FormValue(paramName)
	if formVal == "" {
		return false, nil
	}
	b, err = strconv.ParseBool(formVal)
	if err != nil {
		return b, newParseError(err, paramName)
	}
	return b, nil
}

func newParseError(err error, paramName string) error {
	return fmt.Errorf("unable to parse param '%s': %w", paramName, err)
}
