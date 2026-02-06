# Contributing to MicroRedis

Thank you for your interest in contributing to MicroRedis! This guide covers the development setup, coding conventions, and PR process.

## Development Setup

### Prerequisites

- Python 3.10+ (for development and testing)
- `redis-cli` (optional, for manual testing)
- An ESP32-S3 board (optional, for hardware testing)

### Getting Started

```bash
git clone https://github.com/mateuszsury/microredis.git
cd microredis

# Run unit tests to verify setup
python microredis/tests/runner.py
```

No virtual environment or pip install is needed -- MicroRedis has zero external dependencies.

## MicroPython Constraints

MicroRedis must run on MicroPython. Keep these constraints in mind:

| Feature | Status | Alternative |
|---------|--------|-------------|
| Type hints with `\|` syntax | Not supported | Use comments or `Optional[]` |
| `collections.deque` | Limited (no `len`, no indexing) | Use plain `list` |
| `dataclasses` | Not available | Use `__slots__` classes |
| `match/case` | Not available | Use `if/elif` chains |
| `logging` module | Not available | Use `print()` |
| f-strings | Supported | -- |
| `asyncio` | Available as `uasyncio` | Import `asyncio` (aliased) |
| Exception chaining (`from`) | Not supported | Use simple `raise` |

### Memory Rules

- Use `__slots__` on **every** class
- Never call `list(dict.keys())` on large dicts -- use iteration or reservoir sampling
- Pre-allocate buffers and RESP response constants
- Avoid temporary object creation in hot paths
- Target < 300KB total RAM usage

## Code Style

- **No type annotations in function signatures** (MicroPython compatibility)
- Use docstrings on public methods (brief, one-line where possible)
- Follow existing naming conventions: `snake_case` for functions, `PascalCase` for classes
- Keep imports at the top of the file, grouped: stdlib, then project modules
- Constants in `UPPER_SNAKE_CASE`

## Project Structure

```
microredis/
  core/         # Protocol parsing, response building, constants
  network/      # TCP connections, command routing, middleware
  storage/      # Storage engine, expiry, memory management, data types
  commands/     # Advanced command modules (bitmaps, HLL, streams)
  features/     # Pub/Sub, transactions
  persistence/  # Snapshot save/load, flash storage
  tests/        # Built-in test runner
```

### Where to add new commands

1. If it's a new data type operation, add a method to the appropriate file in `storage/datatypes/`
2. Register the command in `network/router.py` in `_register_all_commands()`
3. Write the handler method in `router.py` following the existing pattern
4. Add unit tests in `tests/runner.py`

## Testing

### Unit Tests

```bash
python microredis/tests/runner.py
```

All 74 tests must pass. Tests run on both CPython and MicroPython.

### Integration Tests

```bash
# Terminal 1: start server
python -c "from microredis.main import run; run()"

# Terminal 2: run tests
python tests/test_simple.py
python tests/test_integration.py
python tests/test_transaction.py
```

### Adding Tests

Add test functions to `microredis/tests/runner.py`. Follow the existing pattern:

```python
def test_your_feature():
    storage = Storage()
    # ... test logic ...
    assert result == expected, f"Expected {expected}, got {result}"
```

## Pull Request Process

1. **Fork** the repository and create a feature branch from `main`
2. **Make your changes** following the coding guidelines above
3. **Run all tests** -- unit tests must be 74/74 (or more, if you added tests)
4. **Write a clear PR description** explaining what changed and why
5. **Keep PRs focused** -- one feature or fix per PR

### PR Checklist

- [ ] All existing tests pass (`python microredis/tests/runner.py`)
- [ ] New functionality has tests
- [ ] Code uses `__slots__` on any new classes
- [ ] No MicroPython-incompatible features used
- [ ] No new external dependencies added
- [ ] CHANGELOG.md updated (if user-facing change)

## Reporting Bugs

Use [GitHub Issues](https://github.com/mateuszsury/microredis/issues) with the bug report template. Include:

- Command that triggers the bug
- Expected vs actual behavior
- Platform (CPython version or MicroPython + board)

## Feature Requests

Use [GitHub Issues](https://github.com/mateuszsury/microredis/issues) with the feature request template. Priority is given to:

- Redis commands that are commonly used
- Memory optimizations
- ESP32-S3 platform improvements

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
