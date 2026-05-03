//go:build js && wasm

package main

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apstndb/spannerplan/plantree/reference"
)

var (
	unmarshalQueryPlan = protojson.UnmarshalOptions{DiscardUnknown: true}
	renderTreeTableJS  js.Func
)

func main() {
	renderTreeTableJS = js.FuncOf(renderTreeTable)
	js.Global().Set("spannerplanRenderTreeTable", renderTreeTableJS)
	select {}
}

func renderTreeTable(this js.Value, args []js.Value) (result any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = map[string]any{
				"error": fmt.Sprintf("panic while rendering query plan: %v", recovered),
			}
		}
	}()

	output, err := renderTreeTableFromArgs(args)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	return map[string]any{"output": output}
}

func renderTreeTableFromArgs(args []js.Value) (string, error) {
	if len(args) == 0 || args[0].IsUndefined() || args[0].IsNull() {
		return "", fmt.Errorf("query plan argument is required")
	}

	plan, err := decodeQueryPlan(args[0])
	if err != nil {
		return "", err
	}

	modeStr, err := optionalStringArg(args, 1, "AUTO", "mode")
	if err != nil {
		return "", err
	}
	mode, err := reference.ParseRenderMode(modeStr)
	if err != nil {
		return "", err
	}

	formatStr, err := optionalStringArg(args, 2, "CURRENT", "format")
	if err != nil {
		return "", err
	}
	format, err := reference.ParseFormat(formatStr)
	if err != nil {
		return "", err
	}

	config, err := decodeRenderConfig(args, 3)
	if err != nil {
		return "", err
	}

	return reference.RenderTreeTableWithConfig(plan.GetPlanNodes(), mode, format, config)
}

func decodeQueryPlan(v js.Value) (*sppb.QueryPlan, error) {
	raw, err := jsonBytes(v)
	if err != nil {
		return nil, fmt.Errorf("decode query plan argument: %w", err)
	}

	var plan sppb.QueryPlan
	if err := unmarshalQueryPlan.Unmarshal(raw, &plan); err != nil {
		return nil, fmt.Errorf("decode query plan JSON: %w", err)
	}
	if len(plan.GetPlanNodes()) == 0 {
		return nil, fmt.Errorf("query plan JSON must contain planNodes")
	}
	return &plan, nil
}

func decodeRenderConfig(args []js.Value, index int) (reference.RenderConfig, error) {
	if len(args) <= index || args[index].IsUndefined() || args[index].IsNull() {
		return reference.RenderConfig{}, nil
	}

	raw, err := jsonBytes(args[index])
	if err != nil {
		return reference.RenderConfig{}, fmt.Errorf("decode config argument: %w", err)
	}

	var config reference.RenderConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return reference.RenderConfig{}, fmt.Errorf("decode config JSON: %w", err)
	}
	return config, nil
}

func optionalStringArg(args []js.Value, index int, fallback, name string) (string, error) {
	if len(args) <= index || args[index].IsUndefined() || args[index].IsNull() {
		return fallback, nil
	}
	if args[index].Type() != js.TypeString {
		return "", fmt.Errorf("%s must be a string", name)
	}
	return args[index].String(), nil
}

func jsonBytes(v js.Value) ([]byte, error) {
	if v.Type() == js.TypeString {
		return []byte(v.String()), nil
	}
	if v.Type() != js.TypeObject {
		return nil, fmt.Errorf("expected JSON string or object, got %s", v.Type())
	}

	uint8Array := js.Global().Get("Uint8Array")
	if uint8Array.Truthy() && v.InstanceOf(uint8Array) {
		b := make([]byte, v.Get("length").Int())
		js.CopyBytesToGo(b, v)
		return b, nil
	}

	stringified, err := stringifyJSON(v)
	if err != nil {
		return nil, err
	}
	return []byte(stringified), nil
}

func stringifyJSON(v js.Value) (jsonStr string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("failed to stringify JavaScript value: %v", recovered)
		}
	}()

	stringified := js.Global().Get("JSON").Call("stringify", v)
	if stringified.Type() != js.TypeString {
		return "", fmt.Errorf("failed to stringify JavaScript value")
	}
	return stringified.String(), nil
}
