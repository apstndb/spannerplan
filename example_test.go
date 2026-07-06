package spannerplan_test

import (
	"errors"
	"fmt"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spannerplan"
)

// ExampleNew_invalidPlan shows how to detect a malformed plan without pinning
// the error message text: errors.Is against ErrInvalidPlan matches any
// validation failure, and errors.As exposes the structured *ValidationError.
func ExampleNew_invalidPlan() {
	// A child link references a node index that does not exist.
	_, err := spannerplan.New([]*sppb.PlanNode{
		{Index: 0, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 99}}},
		{Index: 1},
	})

	fmt.Println("is ErrInvalidPlan:", errors.Is(err, spannerplan.ErrInvalidPlan))
	fmt.Println("is ErrChildLinkIndexOutOfRange:", errors.Is(err, spannerplan.ErrChildLinkIndexOutOfRange))

	var verr *spannerplan.ValidationError
	if errors.As(err, &verr) {
		fmt.Printf("node %d, child link %d\n", verr.NodeIndex, verr.ChildIndex)
	}

	// Output:
	// is ErrInvalidPlan: true
	// is ErrChildLinkIndexOutOfRange: true
	// node 0, child link 0
}
