package http

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"openobserve-jaeger/internal/jaeger_service"
	"openobserve-jaeger/internal/openobserve_service"
	"time"
)

type jaegerServerRoute struct {
	JaegerService *jaeger_service.JaegerService
}

func NewJaegerServer() *jaegerServerRoute {
	return &jaegerServerRoute{
		JaegerService: jaeger_service.NewJaegerService(),
	}
}

// const traceIDParam = "traceID"
func (s *jaegerServerRoute) SearchTraces(ctx *gin.Context) (*jaeger_service.JaegerStructuredResponse, error) {
	jaegerResp := jaeger_service.JaegerStructuredResponse{
		Data:   make([]string, 0),
		Errors: make([]jaeger_service.JaegerStructuredError, 0),
	}

	traceQueryParameters, err := qp.parseTraceQueryParams(ctx, ctx.Request)
	if err != nil {

		jaegerResp.Errors = append(jaegerResp.Errors, jaeger_service.JaegerStructuredError{
			Code: 405,
			Msg:  err.Error(),
		})

		return &jaegerResp, nil
	}

	jaegerResp = s.JaegerService.FindTraces(ctx, &traceQueryParameters.TraceQueryParameters)
	return &jaegerResp, nil
}

func (s *jaegerServerRoute) GetTrace(ctx *gin.Context) (*jaeger_service.JaegerStructuredResponse, error) {
	q, err := valideRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("start_time or end_time is not correct: %v", err)
	}
	log.Printf("valideRequest, q: %v", q)
	jaegerStructuredResponse := s.JaegerService.GetTrace(ctx, q)
	return &jaegerStructuredResponse, nil
}

func (s *jaegerServerRoute) GetService(ctx *gin.Context) (*jaeger_service.JaegerStructuredResponse, error) {

	q, err := valideRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("start_time or end_time is not correct: %v", err)
	}

	jaegerStructuredResponse := s.JaegerService.GetService(ctx, q)

	return &jaegerStructuredResponse, nil
}

func (s *jaegerServerRoute) GetOperations(ctx *gin.Context) (*jaeger_service.JaegerStructuredResponse, error) {
	q, err := valideRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("start_time or end_time is not correct: %v", err)
	}

	jaegerStructuredResponse := s.JaegerService.GetOperations(ctx, q)
	return &jaegerStructuredResponse, nil
}

func valideRequest(ctx *gin.Context) (*openobserve_service.OOQuery, error) {
	// 参数获取
	traceID := ctx.Param("id")
	if len(traceID) > 32 {
		return nil, fmt.Errorf("TraceID cannot be longer than 32 hex characters: %s", traceID)
	}

	servicename := ctx.Param("servicename")
	serviceTag := ctx.Query("service_tag")
	version := ctx.Query("version")

	q := &openobserve_service.OOQuery{
		TraceID:     traceID,
		ServiceName: servicename,
		ServiceTag:  serviceTag,
	}
	if version == "report" {
		q.SearchType = openobserve_service.BackgroundSearchType
	}

	err := ctx.BindQuery(&q)
	if err != nil {
		return nil, fmt.Errorf("start_time or end_time is not correct: %v", err)
	}

	if q.StartTimeUnix > 0 {
		if len(fmt.Sprintf("%d", q.StartTimeUnix)) < 16 {
			q.StartTime = time.Unix(q.StartTimeUnix, 0)
		} else {
			q.StartTime = time.UnixMicro(q.StartTimeUnix)
		}
	}

	if q.EndTimeUnix > 0 {
		if len(fmt.Sprintf("%d", q.EndTimeUnix)) < 16 {
			q.EndTime = time.Unix(q.EndTimeUnix, 0)
		} else {
			q.EndTime = time.UnixMicro(q.EndTimeUnix)
		}
	}

	return q, nil
}
