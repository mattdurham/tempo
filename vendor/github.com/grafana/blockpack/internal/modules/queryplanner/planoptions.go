package queryplanner

// PlanOptions is a blockpack data type.
type PlanOptions struct {
	Limit         int
	Direction     Direction
	EnableExplain bool
}
