package infra

import (
	"encoding/json"
	"fmt"
)

const (
	CommonError          = 00010001
	DeleteEffectRowsZero = 00010002
	JsonIterator         = 00010003
	DBError              = 00010004
	ParamsValidateError  = 00010005
	BusinessParamsError  = 00010006
	WindowNotExists      = 10001001
	TriggerNotExists     = 10001002
	OperatorNotExists    = 10001003
	EvictorNotExists     = 10001004
	CalTaskNotFound      = 10001005
)

var errText = map[int64]string{
	CommonError:          "通用错误",
	DeleteEffectRowsZero: "删除数据影响行数为0",
	JsonIterator:         "JSON序列化失败",
	DBError:              "数据库出错",
	ParamsValidateError:  "参数校验错误",
	BusinessParamsError:  "业务参数错误",
	WindowNotExists:      "窗口资源不存在",
	TriggerNotExists:     "触发器资源不存在",
	OperatorNotExists:    "执行器资源不存在",
	EvictorNotExists:     "剔除器资源不存在",
	CalTaskNotFound:      "找不到计算结果",
}

var EmptyDetail = make(map[string]any)

func ErrText(code int64) string {
	return errText[code]
}

func PanicErr(originErr error, code ...int64) {
	if originErr == nil {
		return
	}
	var errCode int64
	for _, i := range code {
		errCode = i
	}
	if errCode == 0 {
		errCode = CommonError
	}
	panic(NewError(errCode, ErrText(errCode), originErr))
}

type Error struct {
	Code      int64
	Message   string
	Details   map[string]any
	OriginErr error
}

func (e *Error) Error() string {
	b, _ := json.Marshal(e.Details)
	return fmt.Sprintf("code: %d, message: %s, details: %s", e.Code, e.Message, string(b))
}

func (e *Error) SetDetails(details map[string]any) {
	e.Details = details
}

func (e *Error) IsSuccess() bool {
	return e == nil || e.Code == 0
}

func NewError(code int64, message string, originErr error) *Error {
	return &Error{
		Code:      code,
		Message:   message,
		OriginErr: originErr,
		Details:   EmptyDetail,
	}
}
