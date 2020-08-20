package helper

import "context"

// Ctx returns provided context or creates new one.
func Ctx(c context.Context) context.Context {
	if c == nil {
		c = context.Background()
	}

	return c
}
