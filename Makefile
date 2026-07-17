.PHONY: test ecosystem-check ecosystem-render ecosystem-pinned-ref-integrity

test:
	go test -v ./...

ecosystem-check:
	go test -v ./ecosystem
	go run ./ecosystem/cmd/canary

ecosystem-render:
	go run ./ecosystem/cmd/render

ecosystem-pinned-ref-integrity:
	go run ./ecosystem/cmd/canary -live
