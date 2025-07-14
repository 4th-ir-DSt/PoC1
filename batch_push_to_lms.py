import asyncio
import json
import logging
from httpx import AsyncClient, HTTPStatusError
from pydantic import BaseModel
from typing import List, Dict, Any
import time

# Import the models and functions from main.py
from main import (
    Column, Table, Schema, Environment, 
    get_tables, push_table_to_lms, 
    LMS_API_BASE
)

# Configure logging
def setup_logging():
    """Configure logging with both console and file handlers."""
    # Create logger
    logger = logging.getLogger('batch_push_lms')
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # File handler
    file_handler = logging.FileHandler('batch_push_lms.log', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# Setup logger
logger = setup_logging()

# Environment IDs from main.py
ENVIRONMENT_IDS = [
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

# Configuration
STORE_NAME = "lms_store"
INDEX_NAME = "QuestSoftware"
BATCH_SIZE = 5  # Process environments in batches to avoid overwhelming the API
DELAY_BETWEEN_BATCHES = 2  # seconds


class BatchPushResult(BaseModel):
    environment_id: str
    environment_name: str | None = None
    system_name: str | None = None
    total_tables: int = 0
    successful_tables: int = 0
    failed_tables: int = 0
    errors: List[str] = []
    table_results: List[Dict[str, Any]] = []


async def process_environment(env_id: str) -> BatchPushResult:
    """Process a single environment and push all its tables to LMS."""
    result = BatchPushResult(environment_id=env_id)
    
    try:
        logger.info("=" * 60)
        logger.info(f"Processing Environment ID: {env_id}")
        logger.info("=" * 60)
        
        # Get environment data
        env_obj = await get_tables(env_id)
        if not env_obj:
            result.errors.append("Failed to fetch environment data")
            logger.error(f"Failed to fetch environment data for ID: {env_id}")
            return result
        
        result.environment_name = env_obj.name
        result.system_name = env_obj.systemName
        
        # Collect all tables from all schemas
        all_tables = []
        for schema in env_obj.schemas:
            all_tables.extend(schema.tables)
        
        result.total_tables = len(all_tables)
        logger.info(f"Environment: {env_obj.name} ({env_obj.systemName})")
        logger.info(f"Total tables found: {len(all_tables)}")
        
        if not all_tables:
            result.errors.append("No tables found in environment")
            logger.warning(f"No tables found in environment {env_id}")
            return result
        
        # Process each table
        for i, table in enumerate(all_tables, 1):
            try:
                logger.info(f"  [{i}/{len(all_tables)}] Processing table: {table.tableName}")
                table_result = await push_table_to_lms(table, env_obj, STORE_NAME, INDEX_NAME)
                result.table_results.append(table_result)
                result.successful_tables += 1
                logger.info(f"    Successfully pushed: {table.tableName}")
                
            except Exception as e:
                error_msg = f"Failed to push table {table.tableName}: {str(e)}"
                result.errors.append(error_msg)
                result.failed_tables += 1
                result.table_results.append({
                    "tableName": table.tableName,
                    "status": "error",
                    "error": str(e)
                })
                logger.error(f"    Failed to push: {table.tableName} - {str(e)}")
        
        logger.info(f"Environment {env_id} Summary:")
        logger.info(f"   Successful: {result.successful_tables}")
        logger.info(f"   Failed: {result.failed_tables}")
        logger.info(f"   Total: {result.total_tables}")
        
    except Exception as e:
        error_msg = f"Unexpected error processing environment {env_id}: {str(e)}"
        result.errors.append(error_msg)
        logger.error(f"Unexpected error processing environment {env_id}: {str(e)}")
    
    return result


async def batch_push_all_environments():
    """Process all environments in batches and push their tables to LMS."""
    logger.info("Starting batch push of all environments to LMS")
    logger.info(f"Total environments to process: {len(ENVIRONMENT_IDS)}")
    logger.info(f"Configuration: Store={STORE_NAME}, Index={INDEX_NAME}")
    logger.info(f"Batch size: {BATCH_SIZE}, Delay between batches: {DELAY_BETWEEN_BATCHES}s")
    
    all_results = []
    start_time = time.time()
    
    # Process environments in batches
    for i in range(0, len(ENVIRONMENT_IDS), BATCH_SIZE):
        batch = ENVIRONMENT_IDS[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(ENVIRONMENT_IDS) + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info("=" * 80)
        logger.info(f"Processing Batch {batch_num}/{total_batches} ({len(batch)} environments)")
        logger.info("=" * 80)
        
        # Process batch concurrently
        batch_tasks = [process_environment(env_id) for env_id in batch]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # Handle any exceptions from gather
        for j, result in enumerate(batch_results):
            if isinstance(result, Exception):
                env_id = batch[j]
                error_result = BatchPushResult(
                    environment_id=env_id,
                    errors=[f"Task failed with exception: {str(result)}"]
                )
                batch_results[j] = error_result
        
        all_results.extend(batch_results)
        
        # Add delay between batches (except for the last batch)
        if i + BATCH_SIZE < len(ENVIRONMENT_IDS):
            logger.info(f"Waiting {DELAY_BETWEEN_BATCHES} seconds before next batch...")
            await asyncio.sleep(DELAY_BETWEEN_BATCHES)
    
    # Calculate overall statistics
    total_environments = len(all_results)
    successful_environments = sum(1 for r in all_results if r.successful_tables > 0 and r.failed_tables == 0)
    failed_environments = sum(1 for r in all_results if r.failed_tables > 0 or len(r.errors) > 0)
    total_tables_processed = sum(r.total_tables for r in all_results)
    total_tables_successful = sum(r.successful_tables for r in all_results)
    total_tables_failed = sum(r.failed_tables for r in all_results)
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Log final summary
    logger.info("=" * 80)
    logger.info("BATCH PUSH COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Total duration: {duration:.2f} seconds")
    logger.info("Overall Statistics:")
    logger.info(f"   Environments processed: {total_environments}")
    logger.info(f"   Successful environments: {successful_environments}")
    logger.info(f"   Failed environments: {failed_environments}")
    logger.info(f"   Total tables processed: {total_tables_processed}")
    logger.info(f"   Successful tables: {total_tables_successful}")
    logger.info(f"   Failed tables: {total_tables_failed}")
    logger.info(f"   Success rate: {(total_tables_successful/total_tables_processed*100):.1f}%" if total_tables_processed > 0 else "   Success rate: N/A")
    
    # Save detailed results to file
    output_file = "batch_push_results.json"
    results_data = {
        "summary": {
            "total_environments": total_environments,
            "successful_environments": successful_environments,
            "failed_environments": failed_environments,
            "total_tables_processed": total_tables_processed,
            "total_tables_successful": total_tables_successful,
            "total_tables_failed": total_tables_failed,
            "duration_seconds": duration,
            "success_rate_percent": (total_tables_successful/total_tables_processed*100) if total_tables_processed > 0 else None
        },
        "environment_results": [result.model_dump() for result in all_results]
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Detailed results saved to: {output_file}")
    
    return all_results


if __name__ == "__main__":
    # Run the batch push
    asyncio.run(batch_push_all_environments()) 