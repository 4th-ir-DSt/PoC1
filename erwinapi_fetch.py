import asyncio
from fastapi import FastAPI
from httpx import AsyncClient, HTTPStatusError
from pydantic import BaseModel

app = FastAPI()


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

            output = Environment(
                nodeId=data['nodeId'],
                name=data['name'],
                systemId=data['systemId'],
                systemName=data['systemName'],
                schemas=data['schemas'],
            )
            with open("output.json", "w") as f:
                f.write(output.model_dump_json())
            return output
    except HTTPStatusError as e:
        print(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        return None

print(len(environment_ids))



asyncio.run(get_tables("23")) 