# Changelog

## [0.3.0](https://github.com/ThreadifyDev/go-sdk/compare/v0.2.1...v0.3.0) (2026-07-18)

### Added

* Add SDK-managed request timeouts for WebSocket request/response operations and GraphQL queries, with a one-second default.
* Add `WithRequestTimeout` to configure the default while preserving shorter caller deadlines and cancellation.

### Changed

* Allow callers to use `context.Background()` for ordinary SDK operations without manually creating a timeout for every request.
* Centralize WebSocket context validation and request timeout handling at the connection boundary.

### Fixed

* Prevent nil, cancelled, or expired contexts from sending thread and step requests.
* Preserve local subscription state when an unsubscribe request cannot be sent.
* Update the OTEL exporter to attach span references through `ThreadInstance.AddRefs` after the step-level refs API was removed.

## [0.2.0](https://github.com/creativeJoe007/ThreadifyEngine/compare/threadify-sdk-go-v0.1.0...threadify-sdk-go-v0.2.0) (2026-02-19)


### Features

* Configure automated releases for Go and Python SDKs, update Go … ([ac7c46d](https://github.com/creativeJoe007/ThreadifyEngine/commit/ac7c46d658c2bc3b91ceff5d629851d89825d3b9))
* Configure automated releases for Go and Python SDKs, update Go module import path, and add versioning. ([6dbcf36](https://github.com/creativeJoe007/ThreadifyEngine/commit/6dbcf360eeeb0bf7edeab9db9a26c84bdff3da18))
* implemented golang sdk ([17dcacd](https://github.com/creativeJoe007/ThreadifyEngine/commit/17dcacde87ebddb65cef66c2d229f9a92ac9a594))
* improve Go and Python SDK step method chaining by propagating errors internally rather than returning them directly. ([fa3199b](https://github.com/creativeJoe007/ThreadifyEngine/commit/fa3199b419c9415681126a3c17ded69cd9a69ba0))
* Introduce basic usage and notification examples for Python and Go SDKs, alongside new thread tests and CI/CD workflow updates. ([755b3e3](https://github.com/creativeJoe007/ThreadifyEngine/commit/755b3e35cd21b0e76457add21d17db90afff344d))
