## Setting Up Your Development Environment

Please Note: All commands mentioned below should be run from the repository
root.

### 1. Create and Activate a Virtual Environment

Create and activate a virtual environment to manage project dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

### 2. Install Project Dependencies alongside additional Development dependencies

Run the following command to install:

- Main project dependencies:
- Additional dependencies. These dependencies are necessary for running tests,
  linting, and other development tasks.

```bash
pip install ".[dev]"
```

### 3. Install Documentation Dependencies

To build and view the documentation, you need to install the necessary
dependencies. Follow these steps to set up the documentation environment:

Install the documentation dependencies using `pip`. These dependencies are
listed under the `[docs]` optional dependencies in `pyproject.toml`. Run the
following command:

### 4. Contributing to MAWpy:

Please follow the following link to learn about how to do open-source
contribution and git best practices.
https://github.com/uw-ssec/rse-guidelines/blob/main/fundamentals/git-github.md

```bash
pip install ".[docs]"
```

## Running Tests

To ensure your changes are properly tested, follow these steps: Execute the test
suite using `pytest`:

```bash
pytest
```
