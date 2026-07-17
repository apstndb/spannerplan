.PHONY: test ecosystem-check ecosystem-render ecosystem-canary

test:
	go test -v ./...

ecosystem-check:
	go test -v ./ecosystem
	go run ./ecosystem/cmd/canary

ecosystem-render:
	go run ./ecosystem/cmd/render

ecosystem-canary:
	go run ./ecosystem/cmd/canary -live
