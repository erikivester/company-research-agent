# backend/services/mongodb.py
import logging
from datetime import datetime
from typing import Dict, Any
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

logger = logging.getLogger(__name__)

class MongoDBService:
    def __init__(self, uri: str, db_name: str = "company_research"):
        try:
            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # Ping the server to check connection
            self.client.admin.command('ping')
            self.db = self.client[db_name]
            self.jobs_collection = self.db["jobs"]
            self.reports_collection = self.db["reports"]
            logger.info("Successfully connected to MongoDB.")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        except OperationFailure as e:
            logger.error(f"MongoDB authentication failed (if applicable): {e}")
            raise

    def create_job(self, job_id: str, job_details: Dict[str, Any]):
        """Creates a new job record in the database."""
        try:
            job_document = {
                "job_id": job_id,
                "status": "pending",
                "created_at": datetime.now().isoformat(),
                "last_update": datetime.now().isoformat(),
                "details": job_details
            }
            self.jobs_collection.insert_one(job_document)
            logger.debug(f"Created job record for job_id: {job_id}")
        except Exception as e:
            logger.error(f"Failed to create job {job_id} in MongoDB: {e}")

    def update_job(self, job_id: str, status: str, error: str = None):
        """Updates the status of an existing job."""
        try:
            update_query = {
                "$set": {
                    "status": status,
                    "last_update": datetime.now().isoformat()
                }
            }
            if error:
                update_query["$set"]["error"] = error

            self.jobs_collection.update_one({"job_id": job_id}, update_query)
            logger.debug(f"Updated job {job_id} status to {status}")
        except Exception as e:
            logger.error(f"Failed to update job {job_id} in MongoDB: {e}")

    def store_report(self, job_id: str, report_data: Dict[str, Any]):
        """Stores the final generated report."""
        try:
            report_document = {
                "job_id": job_id,
                "generated_at": datetime.now().isoformat(),
                **report_data
            }
            # Use update_one with upsert=True to avoid duplicates
            self.reports_collection.update_one(
                {"job_id": job_id},
                {"$set": report_document},
                upsert=True
            )
            logger.info(f"Stored report for job_id: {job_id}")
        except Exception as e:
            logger.error(f"Failed to store report for job {job_id} in MongoDB: {e}")

    def get_job(self, job_id: str) -> Dict[str, Any]:
        """Retrieves a job record."""
        try:
            return self.jobs_collection.find_one({"job_id": job_id}, {"_id": 0})
        except Exception as e:
            logger.error(f"Failed to retrieve job {job_id} from MongoDB: {e}")
            return None

    def get_report(self, job_id: str) -> Dict[str, Any]:
        """Retrieves a report record."""
        try:
            return self.reports_collection.find_one({"job_id": job_id}, {"_id": 0})
        except Exception as e:
            logger.error(f"Failed to retrieve report for {job_id} from MongoDB: {e}")
            return None