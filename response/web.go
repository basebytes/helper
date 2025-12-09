package response

import (
	"bytes"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gin-gonic/gin"
)

func OK(ctx *gin.Context, data any) {
	ctx.JSON(http.StatusOK, &Response{
		Code: http.StatusOK,
		Data: data,
	})
}

func IndentedOK(ctx *gin.Context, data any) {
	ctx.IndentedJSON(http.StatusOK, &Response{
		Code: http.StatusOK,
		Data: data,
	})
}

func Page(ctx *gin.Context, data any, total int64) {
	ctx.JSON(http.StatusOK, &Response{
		Code:  http.StatusOK,
		Data:  data,
		Total: total,
	})
}

func QueryOK(ctx *gin.Context, data any, total int64, imprecise byte) {
	ctx.JSON(http.StatusOK, &Response{
		Code:      http.StatusOK,
		Data:      data,
		Total:     total,
		Imprecise: imprecise != 0,
	})
}

func BadRequest(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, badRequestResponse)
}

func NoPermission(ctx *gin.Context) {
	ctx.AbortWithStatusJSON(http.StatusOK, noPermissionResponse)
}

func NotFound(ctx *gin.Context) {
	ctx.AbortWithStatusJSON(http.StatusOK, &Response{
		Code: http.StatusNotFound,
		Msg:  "not found",
	})
}

func AbortWithBadRequest(ctx *gin.Context) {
	ctx.AbortWithStatusJSON(http.StatusOK, badRequestResponse)
}

func ConflictRequest(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, dataExistResponse)
}

func ServerError(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, serverErrorResponse)
}

func RequestTimeout(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, timeOutResponse)
}

func AuthFailed(ctx *gin.Context, msg string) {
	ctx.AbortWithStatusJSON(http.StatusOK, &Response{
		Code: http.StatusUnauthorized,
		Msg:  msg,
	})

}

func OperateFailed(ctx *gin.Context, msg string, data ...any) {
	res := &Response{Code: 420, Msg: msg}
	if len(data) > 0 {
		res.Data = data
	}
	ctx.JSON(http.StatusOK, res)
}

func Trans(ctx *gin.Context, result any) {
	ctx.JSON(http.StatusOK, result)
}

func Export(ctx *gin.Context, filename string, buffer *bytes.Buffer, total int) {
	ctx.Header("Content-Type", "application/octet-stream")
	ctx.Header("Content-Disposition", `attachment; filename*=UTF-8''`+url.QueryEscape(filename))
	ctx.Header("Total", strconv.Itoa(total))
	_, _ = ctx.Writer.Write(buffer.Bytes())
}

var (
	badRequestResponse = &Response{
		Code: http.StatusBadRequest,
		Msg:  "参数错误",
	}
	dataExistResponse = &Response{
		Code: http.StatusConflict,
		Msg:  "已存在",
	}
	serverErrorResponse = &Response{
		Code: http.StatusInternalServerError,
		Msg:  "服务器错误",
	}
	timeOutResponse = &Response{
		Code: http.StatusRequestTimeout,
		Msg:  "服务器繁忙",
	}
	noPermissionResponse = &Response{
		Code: http.StatusForbidden,
		Msg:  "无权限",
	}
)

type Response struct {
	Code      int    `json:"code"`
	Msg       string `json:"message,omitempty"`
	Data      any    `json:"data,omitempty"`
	Total     int64  `json:"total,omitempty"`
	Imprecise bool   `json:"imprecise,omitempty"`
}
