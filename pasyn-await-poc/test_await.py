#!/usr/bin/env python3
"""Test script for native Python awaitables implemented in Rust."""

import asyncio
import pasyn_await_poc


async def test_hello_awaitable():
    """Test the simple hello awaitable."""
    print("Testing HelloAwaitable...")
    result = await pasyn_await_poc.hello_awaitable()
    print(f"  Result: {result}")
    assert result == "hello world", f"Expected 'hello world', got {result}"
    print("  PASSED!")


async def test_blocking_awaitable():
    """Test the blocking awaitable that releases GIL."""
    print("Testing BlockingAwaitable...")
    result = await pasyn_await_poc.blocking_awaitable()
    print(f"  Result: {result}")
    assert result == "hello from blocking work", f"Expected 'hello from blocking work', got {result}"
    print("  PASSED!")


async def test_asyncio_awaitable():
    """Test the asyncio-integrated awaitable."""
    print("Testing AsyncIOAwaitable...")
    result = await pasyn_await_poc.asyncio_awaitable()
    print(f"  Result: {result}")
    assert result == "hello from asyncio", f"Expected 'hello from asyncio', got {result}"
    print("  PASSED!")


async def test_concurrent():
    """Test running multiple awaitables concurrently."""
    print("Testing concurrent execution...")
    results = await asyncio.gather(
        pasyn_await_poc.hello_awaitable(),
        pasyn_await_poc.blocking_awaitable(),
        pasyn_await_poc.asyncio_awaitable(),
    )
    print(f"  Results: {results}")
    assert results[0] == "hello world"
    assert results[1] == "hello from blocking work"
    assert results[2] == "hello from asyncio"
    print("  PASSED!")


async def main():
    print("=" * 60)
    print("Native Python Awaitable POC Tests")
    print("=" * 60)
    print()

    await test_hello_awaitable()
    print()

    await test_blocking_awaitable()
    print()

    await test_asyncio_awaitable()
    print()

    await test_concurrent()
    print()

    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
