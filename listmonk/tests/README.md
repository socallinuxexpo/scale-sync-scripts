# Tests for scale_email_sync

This directory contains tests for the `scale_email_sync.py` script.

## Running Tests

### Install test dependencies

```bash
pip install -r requirements-test.txt
```

### Run all tests

```bash
pytest
```

### Run with coverage

```bash
pytest --cov=scale_email_sync --cov-report=html
```

### Run specific test file

```bash
pytest tests/test_scale_email_sync.py
```

### Run specific test class or function

```bash
pytest tests/test_scale_email_sync.py::TestListMonk
pytest tests/test_scale_email_sync.py::TestListMonk::test_add_subscriber_new
```

### Run with verbose output

```bash
pytest -v
```

## Test Structure

- `test_scale_email_sync.py` - Main test file containing:
   - `TestLoadConfig` - Tests for configuration loading
   - `TestRegData` - Tests for RegData class (CSV and database operations)
   - `TestListMonk` - Tests for ListMonk class (API interactions,
     synchronization)
   - `TestIntegration` - Integration tests for the full workflow

## Coverage

The tests aim to cover:

- Configuration file loading
- CSV data parsing
- Database data fetching
- Listmonk API interactions (GET, POST, PUT)
- Subscriber synchronization logic
- List management (expected, missing, extra lists)
- Dry-run mode
- Stats tracking and Datadog reporting
- Error handling
