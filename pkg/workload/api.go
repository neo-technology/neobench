package workload

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"go.uber.org/zap"
)

type Workload interface {
	Initialize(driver neo4j.Driver, logger *zap.SugaredLogger) error
	Next() UnitOfWork
}

type UnitOfWork struct {
	Statements []Statement
}

type Statement struct {
	Query string
	Params map[string]interface{}
}