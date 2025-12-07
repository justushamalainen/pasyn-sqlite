# pasyn-await-poc

Proof of concept for implementing native Python awaitables in Rust using PyO3.

This module demonstrates different approaches to creating Python awaitable objects from Rust that integrate with Python's asyncio event loop.

## Purpose

This is an experimental module used to explore different techniques for:

- Creating native Python awaitables in Rust
- Releasing the GIL during blocking operations
- Integrating with Python's asyncio event loop

The learnings from this POC inform the design of `pasyn-sqlite-core`.

## Building

```bash
cd pasyn-await-poc
pip install maturin
maturin develop
```

## Testing

```bash
python test_await.py
```

## Awaitables Provided

- `hello_awaitable()` - Simple awaitable that returns immediately
- `blocking_awaitable()` - Awaitable that releases GIL during blocking work
- `asyncio_awaitable()` - Awaitable that integrates with asyncio's event loop

## Note

This is a proof-of-concept module and is not intended for production use. See `pasyn-sqlite-core` for the production implementation.
