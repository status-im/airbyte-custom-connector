import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Iterable, Tuple, Any, Union

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode, AirbyteCatalog, AirbyteStream

# Report IDs for App Store Analytics API (see reports.csv for more details)
APP_INSTALL_PERFORMANCE_REPORT_ID = "r5-1032fee7-dfb3-4a4a-b24d-e603c95f5b09"
APP_DOWNLOADS_DETAILED_REPORT_ID = "r4-1032fee7-dfb3-4a4a-b24d-e603c95f5b09"
APP_INSTALLATION_DELETION_DETAILED_REPORT_ID = "r7-1032fee7-dfb3-4a4a-b24d-e603c95f5b09"
APP_SESSIONS_DETAILED_REPORT_ID = "r9-1032fee7-dfb3-4a4a-b24d-e603c95f5b09"
APP_DISCOVERY_ENGAGEMENT_DETAILED_REPORT_ID = "r15-1032fee7-dfb3-4a4a-b24d-e603c95f5b09"

from .utils import (
    create_jwt_token,
    get_report_instances,
    download_report_instance,
    read_and_filter_report
)

logger = logging.getLogger("airbyte")

class SourceAppStore(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            key_id = config.get("key_id")
            issuer_id = config.get("issuer_id")
            private_key = config.get("private_key")
            
            if not all([key_id, issuer_id, private_key]):
                return False, "Missing required fields: key_id, issuer_id, or private_key"
            
            # Create token to verify credentials are valid
            token = create_jwt_token(key_id, issuer_id, private_key)
            if not token:
                return False, "Failed to create JWT token with provided credentials"
            
            # Test the API connection
            api_config = {"key_id": key_id, "issuer_id": issuer_id, "private_key": private_key}
            instances = get_report_instances(APP_INSTALL_PERFORMANCE_REPORT_ID, api_config)
            
            if instances and "data" in instances:
                return True, None
            else:
                return False, "Unable to fetch report instances. Please check your credentials."
                
        except Exception as e:
            return False, f"Error connecting to App Store Connect: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # Define stream configurations
        stream_configs = [
            {
                "name": "app_install_performance",
                "report_id": APP_INSTALL_PERFORMANCE_REPORT_ID,
                "report_name": "App Install Performance"
            },
            {
                "name": "app_downloads_detailed",
                "report_id": APP_DOWNLOADS_DETAILED_REPORT_ID,
                "report_name": "App Downloads Detailed"
            },
            {
                "name": "app_installation_deletion_detailed",
                "report_id": APP_INSTALLATION_DELETION_DETAILED_REPORT_ID,
                "report_name": "App Store Installation and Deletion Detailed"
            },
            {
                "name": "app_sessions_detailed", 
                "report_id": APP_SESSIONS_DETAILED_REPORT_ID,
                "report_name": "App Sessions Detailed"
            },
            {
                "name": "app_discovery_engagement_detailed",
                "report_id": APP_DISCOVERY_ENGAGEMENT_DETAILED_REPORT_ID,
                "report_name": "App Store Discovery and Engagement Detailed"
            }
        ]
        
        # Create stream instances using the configurations
        return [AppReportStream(config, stream_config) for stream_config in stream_configs]

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        """Override the default discover method to ensure all streams are added to the catalog"""
        streams = self.streams(config)
        
        catalog_streams = []
        for stream in streams:
            schema = stream.get_json_schema()
            catalog_streams.append(
                AirbyteStream(
                    name=stream.name,
                    json_schema=schema,
                    supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
                    source_defined_cursor=True,
                    default_cursor_field=["Date"]
                )
            )
        return AirbyteCatalog(streams=catalog_streams)

class AppReportStream(Stream):
    """Base stream for App Store reports"""
    
    def __init__(self, config: Mapping[str, Any], stream_config: Mapping[str, Any] = None):
        self.key_id = config.get("key_id")
        self.issuer_id = config.get("issuer_id")
        self.private_key = config.get("private_key")
        self.start_date = config.get("start_date") 
        self.end_date = config.get("end_date")
        
        # Create a temporary directory for downloads
        self.temp_dir = tempfile.mkdtemp(prefix="app_store_")
        
        # Set stream-specific properties if provided
        if stream_config:
            self._name = stream_config["name"]
            self.report_id = stream_config["report_id"]
            self.report_name = stream_config["report_name"]
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return None
    
    def get_json_schema(self) -> Mapping[str, Any]:
        schema_path = os.path.join(os.path.dirname(__file__), "schemas", f"{self.name}.json")
        with open(schema_path, "r") as f:
            return json.load(f)
    
    def download_report(self, instance_id: str, report_name: str) -> Optional[str]:
        """Download a report for a specific instance ID"""
        api_config = {
            "key_id": self.key_id,
            "issuer_id": self.issuer_id,
            "private_key": self.private_key
        }
        downloaded_files = set()
        
        try:
            file_path = download_report_instance(
                instance_id=instance_id,
                report_name=report_name, 
                processing_date="",
                downloaded_files=downloaded_files,
                config=api_config
            )
            
            if not file_path or not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                logger.error(f"Failed to download report for instance {instance_id}")
                return None
                
            logger.info(f"Successfully downloaded report to: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error downloading report: {str(e)}")
            return None
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        # Calculate date range
        end_date = datetime.now()
        if self.end_date:
            try:
                end_date = datetime.strptime(self.end_date, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"Invalid end_date format: {self.end_date}, using current date")
                
        # Default to 3 days before end_date if start_date not provided
        if self.start_date:
            try:
                start_date = datetime.strptime(self.start_date, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"Invalid start_date format: {self.start_date}, using end_date - 1 day")
                start_date = end_date - timedelta(days=3)
        else:
            start_date = end_date - timedelta(days=3)

        # Generate daily slices
        slices = []
        current_date = start_date
        while current_date <= end_date:
            slices.append({"date": current_date.strftime("%Y-%m-%d")})
            current_date += timedelta(days=1)
            
        return slices
    
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Dict[str, Any]] = None,
        stream_state: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        date = stream_slice.get("date", datetime.now().strftime("%Y-%m-%d")) if stream_slice else datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Processing date: {date}")
        
        api_config = {"key_id": self.key_id, "issuer_id": self.issuer_id, "private_key": self.private_key}
        
        instances = get_report_instances(self.report_id, api_config)
        if not instances or "data" not in instances:
            logger.warning(f"No report instances found for date {date}")
            return
        
        # Filter instances for this date
        instances_for_date = [
            instance for instance in instances["data"] 
            if instance.get('id') and 'attributes' in instance and instance['attributes'].get('processingDate', '') == date
        ]
        if not instances_for_date:
            return
        
        # Process each matching instance
        for instance in instances_for_date:
            instance_id = instance["id"]
            processing_date = instance["attributes"]["processingDate"]
            
            # Calculate expected date (processing_date - 1 day)
            try:
                processing_date_obj = datetime.strptime(processing_date, "%Y-%m-%d")
                expected_date = (processing_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info(f"Filtering for records with Date = {expected_date}")
            except ValueError:
                logger.error(f"Could not parse processing date: {processing_date}")
                continue
            
            file_path = self.download_report(instance_id, self.report_name)
            if not file_path:
                continue
            
            records = read_and_filter_report(file_path, expected_date)
            for record in records:
                yield record