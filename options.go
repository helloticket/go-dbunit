package dbunit

type Options func(o *options)

type options struct {
	sql bool
}

func DebugSQL() Options {
	return func(o *options) {
		o.sql = true
	}
}
