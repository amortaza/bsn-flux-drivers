package stdsql

type SelectCompiler interface {
	Compile() (string, error)
}
