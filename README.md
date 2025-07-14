# Logical Data Modeling Assistant API

A production-ready FastAPI backend that leverages LLMs to help users design, refine, and explain logical data models through a conversational interface.

## Features

- Conversational assistant for logical data modeling
- Only generates models after explicit user approval or detailed requirements
- Flexible response format (text or JSON)
- Robust logging, error handling, and configuration management
- Modular, scalable, and ready for production deployment

## Project Structure

```
.
├── api/v1/routers/         # FastAPI routers (conversation, health)
├── core/                   # Core business logic, config, logging, storage, prompts
├── schemas/                # Pydantic schemas for API and data models
├── logs/                   # Application and error logs
├── main.py                 # FastAPI application entrypoint
├── pyproject.toml          # Project configuration and dependencies
└── README.md               # Project documentation
```

## Prerequisites

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Setup

1. **Clone the repository:**
   ```sh
   git clone <your-repo-url>
   cd PoC1
   ```

2. **Install uv (if not already installed):**
   ```sh
   # On macOS and Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # On Windows
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

3. **Create and activate a virtual environment with uv:**
   ```sh
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

4. **Install dependencies:**
   ```sh
   uv sync
   ```

5. **Configure environment variables:**
   - Copy `.env.example` to `.env` and update values as needed (LLM API keys, etc).

6. **Run the application:**
   ```sh
   uv run uvicorn main:app --reload
   ```

7. **Access the API:**
   - Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)
   - Health check: [http://localhost:8000/api/v1/health](http://localhost:8000/api/v1/health)

## Development

**Install development dependencies:**
```sh
uv sync --dev
```

**Run tests:**
```sh
uv run pytest
```

**Format code:**
```sh
uv run black .
uv run isort .
```

**Lint code:**
```sh
uv run flake8
uv run mypy .
```

## Docker

**Build and run with Docker:**
```sh
# Build the image
docker build -t logical-data-modeling-assistant .

# Run the container
docker run -p 8000:8000 logical-data-modeling-assistant
```



**For Azure Container Apps:**
```sh
# Build and push to Azure Container Registry
az acr build --registry <your-registry> --image logical-data-modeling-assistant .

# Deploy to Container Apps
az containerapp create \
  --name logical-data-modeling-assistant \
  --resource-group <your-rg> \
  --environment <your-environment> \
  --image <your-registry>.azurecr.io/logical-data-modeling-assistant:latest \
  --target-port 8000 \
  --ingress external \
  --query properties.configuration.ingress.fqdn
```

## API Endpoints

- `POST /api/v1/model-chat/` — Main chat endpoint
- `POST /api/v1/model-chat/reset` — Reset chat history
- `GET /api/v1/model-chat/history` — Get chat history
- `GET /api/v1/health/` — Health check

## License

MIT
