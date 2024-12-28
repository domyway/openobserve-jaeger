package http

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"openobserve-jaeger/internal/jaeger_service"
)

type Hanlder func(ctx *gin.Context) (*jaeger_service.JaegerStructuredResponse, error)

func wrapResponse(h Hanlder) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		response, err := h(ctx)
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if len(response.Errors) > 0 {
			ctx.JSON(response.Errors[0].Code, response)
			return
		}

		ctx.JSON(http.StatusOK, response)
	}
}
func NewHTTPServer() *gin.Engine {
	j := NewJaegerServer()

	engine := gin.Default()

	engine.GET("/api/traces", wrapResponse(j.SearchTraces))
	engine.GET("/api/traces/:id", wrapResponse(j.GetTrace))
	engine.GET("/api/services", wrapResponse(j.GetService))
	engine.GET("/api/services/:servicename/operations", wrapResponse(j.GetOperations))
	return engine
}
