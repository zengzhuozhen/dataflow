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
	WindowNotExists      = 10001001
	TriggerNotExists     = 10001002
	OperatorNotExists    = 10001003
	EvictorNotExists     = 10001004
)

var errText = map[int]string{
	CommonError:          "通用错误",
	DeleteEffectRowsZero: "删除数据影响行数为0",
	JsonIterator:         "JSON序列化失败",
	DBError:              "数据库出错",
	WindowNotExists:      "窗口资源不存在",
	TriggerNotExists:     "触发器资源不存在",
	OperatorNotExists:    "执行器资源不存在",
	EvictorNotExists:     "剔除器资源不存在",
}

var EmptyDetail = make(map[string]any)

func ErrText(code int) string {
	return errText[code]
}

func PanicErr(originErr error, code ...int) {
	if originErr == nil {
		return
	}
	var errCode int
	for _, i := range code {
		errCode = i
	}
	if errCode == 0 {
		errCode = CommonError
	}
	panic(NewError(int32(errCode), ErrText(errCode), originErr))
}

type Error struct {
	Code      int32
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
	return e.Code == 0
}

func NewError(code int32, message string, originErr error) *Error {
	return &Error{
		Code:      code,
		Message:   message,
		OriginErr: originErr,
		Details:   EmptyDetail,
	}
}
