package spannerplan

import (
	"encoding/json"
	"errors"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apstndb/spannerplan/protoyaml"
)

func ExtractQueryPlan(b []byte) (*sppb.ResultSetStats, *sppb.StructType, error) {
	j, err := protoyaml.YAMLToJSON(b)
	if err != nil {
		return nil, nil, err
	}

	var topLevel struct {
		QueryPlan json.RawMessage `json:"queryPlan"`
		PlanNodes json.RawMessage `json:"planNodes"`
		Stats     json.RawMessage `json:"stats"`
	}
	if err := json.Unmarshal(j, &topLevel); err != nil {
		return nil, nil, err
	}

	unmarshalOptions := protojson.UnmarshalOptions{DiscardUnknown: true}

	if len(topLevel.QueryPlan) != 0 {
		var rss sppb.ResultSetStats
		if err := unmarshalOptions.Unmarshal(j, &rss); err != nil {
			return nil, nil, err
		}
		return &rss, nil, nil
	} else if len(topLevel.PlanNodes) != 0 {
		var qp sppb.QueryPlan
		if err := unmarshalOptions.Unmarshal(j, &qp); err != nil {
			return nil, nil, err
		}
		return &sppb.ResultSetStats{QueryPlan: &qp}, nil, nil
	} else if len(topLevel.Stats) != 0 {
		var rs sppb.ResultSet
		if err := unmarshalOptions.Unmarshal(j, &rs); err != nil {
			return nil, nil, err
		}
		return rs.GetStats(), rs.GetMetadata().GetRowType(), nil
	}
	return nil, nil, errors.New("unknown input format")
}
