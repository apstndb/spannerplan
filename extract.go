package spannerplan

import (
	"encoding/json"
	"errors"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/protoyaml"
)

func ExtractQueryPlan(b []byte) (*sppb.ResultSetStats, *sppb.StructType, error) {
	j, err := protoyaml.YAMLToJSON(b)
	if err != nil {
		return nil, nil, err
	}

	// protojson emits either JSON names (queryPlan, planNodes) or the original
	// protobuf names (query_plan, plan_nodes) when UseProtoNames is set.
	// stats is spelled the same in both. Detect either spelling, then decode
	// the original bytes so duplicate aliases and malformed values are still
	// rejected by the decoder. Precedence is unchanged: ResultSetStats, then
	// a bare QueryPlan, then a ResultSet.
	var topLevel map[string]json.RawMessage
	if err := json.Unmarshal(j, &topLevel); err != nil {
		return nil, nil, err
	}

	switch {
	case len(topLevel["queryPlan"]) != 0 || len(topLevel["query_plan"]) != 0:
		var rss sppb.ResultSetStats
		if err := protoyaml.UnmarshalJSON(j, &rss); err != nil {
			return nil, nil, err
		}
		return &rss, nil, nil
	case len(topLevel["planNodes"]) != 0 || len(topLevel["plan_nodes"]) != 0:
		var qp sppb.QueryPlan
		if err := protoyaml.UnmarshalJSON(j, &qp); err != nil {
			return nil, nil, err
		}
		return &sppb.ResultSetStats{QueryPlan: &qp}, nil, nil
	case len(topLevel["stats"]) != 0:
		var rs sppb.ResultSet
		if err := protoyaml.UnmarshalJSON(j, &rs); err != nil {
			return nil, nil, err
		}
		return rs.GetStats(), rs.GetMetadata().GetRowType(), nil
	default:
		return nil, nil, errors.New("unknown input format")
	}
}
