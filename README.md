# GovContract-Alpha Backend

FastAPI backend for government contract signal detection.

## Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` and configure:

- `DATABASE_URL` - PostgreSQL connection string
- `SAM_GOV_API_KEY` - Your SAM.gov API key (optional for mock mode)

## Running

```bash
uvicorn main:app --reload
```

API docs: <http://localhost:8000/docs>
