import asyncio
from fastapi import FastAPI, HTTPException
from httpx import AsyncClient, HTTPStatusError
from pydantic import BaseModel
import json

app = FastAPI()

LMS_API_BASE = "https://language-model-service.mangobeach-c18b898d.switzerlandnorth.azurecontainerapps.io"


class PushRequest(BaseModel):
    env_id: str
    store_name: str = "lms_store"
    index_name: str = "QuestSoftware"

class Column(BaseModel):
    columnName: str
    columnDatatype: str


class Table(BaseModel):
    tableName: str
    tableComments: str | None = None
    columns: list[Column]


class Schema(BaseModel):
    tables: list[Table]


class Environment(BaseModel):
    nodeId: str
    name: str
    systemId: int
    systemName: str
    schemas: list[Schema]


environment_ids = [
    "1", "2", "445", "460", "522", "111", "112", "412", "442", "4", "23", "11", "12",
    "415", "416", "498", "502", "515", "516", "517", "518", "519", "520", "521", "572", "575", "633", "652",
    "19", "431", "432", "434", "435", "20", "22", "82", "406", "407", "612", "25", "32", "104", "117", "232",
    "113", "114", "116", "501", "135", "133", "134", "436", "202", "335", "504", "405", "429", "430", "433",
    "463", "466", "465", "467", "494", "495", "503", "505", "535", "537", "538", "539", "540", "541", "542",
    "544", "573", "578", "579", "580", "593", "600", "613", "614", "666", "667", "550", "551", "552", "553",
    "554", "582", "556", "557", "558", "559", "560", "561", "562", "564", "565", "566", "563", "567", "568",
    "569", "570", "588", "590", "591", "615", "616", "617", "621", "625", "623", "622", "619", "626", "624", "620",
    "628", "627", "638", "630", "635", "634", "639", "644", "647", "648", "650", "651", "663", "662", "661",
    "664", "665", "668"
]


async def get_tables(environment_id: str) -> Environment | None:
    try:
        print(f"Processing environment ID: {environment_id}")
        async with AsyncClient(timeout=None) as client:

            headers = {"Authorization": "185bc7b0-2069-4988-9284-7a92bc3bc0d9"}
            params = {
                "environmentIds": environment_id,
                "fillOptions": 256
            }
            response = await client.get("http://51.103.210.156:8080/erwinDISuite/api/metadatamanager/environments",
                                        params=params, headers=headers)
            response.raise_for_status()
            data = response.json()['data'][0]

            env = Environment(
                nodeId=data['nodeId'],
                name=data['name'],
                systemId=data['systemId'],
                systemName=data['systemName'],
                schemas=data['schemas'],
            )
            print(env)
            return env
    except HTTPStatusError as e:
        print(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        return None


async def push_table_to_lms(table: Table, environment: Environment, store_name: str, index_name: str) -> dict:
    """Push a single table to the LMS."""
    # Create payload with just the single table
    table_data = {
        "table": table.model_dump(),
        "environment": {
            "nodeId": environment.nodeId,
            "name": environment.name,
            "systemId": environment.systemId,
            "systemName": environment.systemName
        }
    }

    payload = [
        {
            "page_content": json.dumps(table_data),
            "metadata": {
                "index_name": index_name,
                "metadata": {
                    "systemName": environment.systemName,
                    "environmentName": environment.name,
                    "tableName": table.tableName,
                    "tableComments": table.tableComments,
                    "columnCount": len(table.columns)
                }
            }
        }
    ]

    # Push payload to LMS API
    lms_url = f"{LMS_API_BASE}/api/v1/vector-store/{store_name}/index/{index_name}/add"
    async with AsyncClient(timeout=None) as client:
        lms_response = await client.post(lms_url, json=payload)
        lms_response.raise_for_status()
        return {
            "tableName": table.tableName,
            "status": "success",
            "columnCount": len(table.columns),
            "lms_response": lms_response.json()
        }


@app.post("/push-to-lms/")
async def push_to_lms(req: PushRequest):
    
    env_obj = await get_tables(req.env_id)
    if not env_obj:
        raise HTTPException(status_code=404, detail="Environment not found or fetch failed.")

    all_tables = []
    for schema in env_obj.schemas:
        all_tables.extend(schema.tables)
    
    if not all_tables:
        raise HTTPException(status_code=404, detail="No tables found in environment.")

    results = []
    for table in all_tables:
        try:
            result = await push_table_to_lms(table, env_obj, req.store_name, req.index_name)
            results.append(result)
            print(f"Successfully pushed table: {table.tableName}")
        except Exception as e:
            error_result = {
                "tableName": table.tableName,
                "status": "error",
                "error": str(e)
            }
            results.append(error_result)
            print(f"Failed to push table {table.tableName}: {e}")

    return {
        "message": f"Processed {len(all_tables)} tables from environment '{env_obj.name}'",
        "environment": {
            "name": env_obj.name,
            "systemName": env_obj.systemName,
            "totalTables": len(all_tables)
        },
        "results": results
    }