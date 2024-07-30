package record

import (
	"slices"

	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

var _ QueryPlanner = HeuristicsQueryPlanner{}

// HeuristicsQueryPlanner uses heuristics to pick a good enough query plan.
// Join order is chosen according to the table that produces the smallest output.
type HeuristicsQueryPlanner struct {
	mdm *MetadataManager
}

func NewHeuristicsQueryPlanner(mdm *MetadataManager) HeuristicsQueryPlanner {
	return HeuristicsQueryPlanner{
		mdm: mdm,
	}
}

func (hqp HeuristicsQueryPlanner) CreatePlan(data sql.Query, x tx.Transaction) (Plan, error) {
	var planners []*tablePlanner
	// create a table planner for each table contained in the query
	for _, tname := range data.Tables() {
		planner, err := newTablePlanner(x, tname, data.Predicate(), hqp.mdm)
		if err != nil {
			return nil, err
		}

		planners = append(planners, planner)
	}

	// choose the lowest-size plan to begin the join order
	plan, planners := lowestSelectPlan(planners)

	for {
		if len(planners) == 0 {
			break
		}

		var p Plan
		p, planners = lowestJoinPlan(planners, plan)
		if p != nil {
			plan = p
		} else {
			plan, planners = lowestProductPlan(planners, plan)
		}
	}

	proj := newProjectPlan(plan, data.Fields())
	if data.OrderByFields() == nil {
		return proj, nil
	}

	return newSortPlan(x, proj, data.OrderByFields()), nil
}

// lowestSelectPlan picks the table that
func lowestSelectPlan(planners []*tablePlanner) (Plan, []*tablePlanner) {
	var bestPlan Plan
	bi := -1
	for i, planner := range planners {
		plan := planner.makeSelectPlan()
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestPlan = plan
			bi = i
		}
	}

	planners = slices.Delete(planners, bi, bi+1)

	return bestPlan, planners
}

func lowestJoinPlan(planners []*tablePlanner, current Plan) (Plan, []*tablePlanner) {
	var bestPlan Plan
	bi := -1
	for i, planner := range planners {
		plan := planner.makeJoinPlan(current)
		if plan == nil {
			continue
		}

		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestPlan = plan
			bi = i
		}
	}

	if bestPlan != nil {
		planners = slices.Delete(planners, bi, bi+1)
	}

	return bestPlan, planners
}

func lowestProductPlan(planners []*tablePlanner, current Plan) (Plan, []*tablePlanner) {
	var bestPlan Plan
	bi := -1

	for i, planner := range planners {
		plan := planner.makeProductPlan(current)
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestPlan = plan
			bi = i
		}
	}

	planners = slices.Delete(planners, bi, bi+1)

	return bestPlan, planners
}

// tablePlanner encapsulates the necessary information and components required to create
// an efficient query plan for the table, based on the given predicate and available indexes.
// tablePlanner uses heuristics to determine the lowest cost query exectuion plan
type tablePlanner struct {
	plan      tablePlan
	predicate Predicate
	schema    Schema
	indexes   map[string]*indexInfo
	x         tx.Transaction
}

func newTablePlanner(x tx.Transaction, tableName string, pred Predicate, mdm *MetadataManager) (*tablePlanner, error) {
	plan, err := newTablePlan(x, tableName, mdm)
	if err != nil {
		return nil, err
	}

	iinfo, err := mdm.indexInfo(x, tableName)
	if err != nil {
		return nil, err
	}

	return &tablePlanner{
		plan:      plan,
		predicate: pred,
		schema:    plan.Schema(),
		indexes:   iinfo,
		x:         x,
	}, nil
}

func (tp tablePlanner) addSelectPredicate(p Plan) Plan {
	selectPredicate, ok := tp.predicate.SelectSubPredicate(tp.schema)
	if ok {
		return newSelectPlan(p, selectPredicate)
	}

	return p
}

func (tp tablePlanner) addJoinPredicate(p Plan, schema Schema) Plan {
	joinedSchema := newJoinedSchema(tp.schema, schema)
	if joinPredicate, ok := tp.predicate.JoinSubPredicate(joinedSchema, tp.schema, schema); ok {
		return newSelectPlan(p, joinPredicate)
	}

	return p
}

// makeSelectPlan creates a Plan that applies the selection predicate to the table.
// It first tries to determine if an index can be use, by attempting to create an IndexSelectPlan
// If such plan cannot be created, it falls back to the original table plan.
// Finally, it determines the portion of the predicate that applies to the table
// and creates a SelectPlan for it.
func (tp tablePlanner) makeSelectPlan() Plan {
	p := tp.makeIndexSelectPlan()
	if p == nil {
		p = tp.plan
	}

	return tp.addSelectPredicate(p)
}

// makeIndexSelectPlan attempts to create an IndexSelectPlan for the underlying table.
// It iterates over the available table indexes and checks if
// the predicate contains an equality condition with a constant value for any of the indexed fields.
// If such a condition is found, it creates and returns an IndexSelectPlan
// using the corresponding index and the constant value.
// If no suitable equality condition is found, the indexSelectPlan cannot be created.
func (tp tablePlanner) makeIndexSelectPlan() Plan {
	for field, ii := range tp.indexes {
		if val, ok := tp.predicate.EquatesWithConstant(field); ok {
			return NewIndexSelectPlan(tp.plan, ii, val)
		}
	}

	return nil
}

// makeJoinPlan checks if a join exists between the specified Plan and this plan.
// If a join predicate exists, the heuristics will attempt to create an IndexJoin.
// If that's not possible, the planner will select a product join plan
// (A product followed by a select).
func (tp tablePlanner) makeJoinPlan(current Plan) Plan {
	schema := current.Schema()
	joinedSchema := newJoinedSchema(tp.schema, schema)

	if _, ok := tp.predicate.JoinSubPredicate(joinedSchema, tp.schema, schema); !ok {
		return nil
	}

	plan := tp.makeIndexJoinPlan(current, schema)
	if plan == nil {
		plan = tp.makeProductJoinPlan(current, schema)
	}

	return plan
}

func (tp tablePlanner) makeProductPlan(current Plan) Plan {
	plan := tp.addSelectPredicate(tp.plan)

	return newProductPlan(tp.plan, plan)
}

func (tp tablePlanner) makeIndexJoinPlan(current Plan, schema Schema) Plan {
	for k, ii := range tp.indexes {
		if f, ok := tp.predicate.EquatesWithField(k); ok && schema.HasField(f) {
			indexJoinPlan := newIndexJoinPlan(current, tp.plan, ii, f)
			p := tp.addSelectPredicate(indexJoinPlan)

			return tp.addJoinPredicate(p, schema)
		}
	}

	return nil
}

func (tp tablePlanner) makeProductJoinPlan(current Plan, schema Schema) Plan {
	p := tp.makeProductPlan(current)

	return tp.addJoinPredicate(p, schema)
}
