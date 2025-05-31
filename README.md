# Grafo Udelar

Repositorio con crawlers y utilidades para crear el grafo de colaboraciones académicas de UdelaR.

## Setup

### Prerequisites

- Python 3.11 or higher
- [UV](https://github.com/astral-sh/uv) package manager

### Installing UV

1. Install UV using curl:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Add UV to your PATH (if not already done):
```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

### Setting up the development environment

1. Clone the repository:
```bash
git clone git@github.com:j-oflaherty/bdnr-proyecto-final.git
cd bdnr-proyecto-final
```

2. Create and activate a virtual environment using UV:
```bash
uv venv
source .venv/bin/activate
```

3. Install dependencies:
```bash
uv sync --all-groups
```

## Usage

The project provides a CLI tool called `udegraph`. You can use it as follows:

```bash
# Show help
udegraph --help

# Run specific commands
udegraph <command> [options]
```
