package record

type Scan interface {
	BeforeFirst()

	Next() error

	GetInt(fname string) (int, error)

	GetString(fname string) (string, error)

	GetVal(fname string) (Constant, error)

	HasField(fname string) bool

	Close()
}

type UpdateScan interface {
	Scan

	SetInt(fname string, v int) error

	SetString(fname string, v string) error

	SetVal(fname string, v Constant) error

	Insert() error

	Delete() error

	GetRid() RID

	MoveToRID(rid RID)
}