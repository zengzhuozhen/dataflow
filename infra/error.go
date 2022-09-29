package infra

import (
	"encoding/json"
	"fmt"
)

const (
	CommonError     = 00010001
	WindowNotExists = 10001001
)

var errText = map[int]string{
	CommonError:     "通用错误",
	WindowNotExists: "窗口资源不存在",
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
