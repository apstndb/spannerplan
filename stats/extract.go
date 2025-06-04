package stats

import (
	"bytes"
	"encoding/json"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
)

func Extract(node *spannerpb.PlanNode, disallowUnknownFields bool) (*ExecutionStats, error) {
	var executionStats ExecutionStats
	if err := jsonRoundtrip(node.GetExecutionStats(), &executionStats, disallowUnknownFields); err != nil {
		return nil, err
	}
	return &executionStats, nil
}

func jsonRoundtrip(input interface{}, output interface{}, disallowUnknownFields bool) error {
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if disallowUnknownFields {
		dec.DisallowUnknownFields()
	}
	err = dec.Decode(output)
	if err != nil {
		return err
	}
	return nil
}
