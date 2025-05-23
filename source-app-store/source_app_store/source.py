import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Iterable, Tuple, Any, Union

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode, AirbyteCatalog, AirbyteStream

from .utils import (
    create_jwt_token,
    get_report_instances,
    download_report_instance,
    read_and_filter_report
)

# Constants
DATE_FORMAT = "%Y-%m-%d"

logger = logging.getLogger("airbyte")

class SourceAppStore(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            key_id = config.get("key_id")
            issuer_id = config.get("issuer_id")
            private_key = config.get("private_key")
            report_ids = config.get("report_ids")
            
            if not all([key_id, issuer_id, private_key, report_ids]):
                return False, "Missing required fields: key_id, issuer_id, private_key, or report_ids"
            
            required_report_ids = [
                "app_install_performance",
                "app_downloads_detailed",
                "app_installation_deletion_detailed",
                "app_sessions_detailed",
                "app_discovery_engagement_detailed"
            ]
            
            missing_report_ids = [rid for rid in required_report_ids if rid not in report_ids]
            if missing_report_ids:
                return False, f"Missing required report IDs: {', '.join(missing_report_ids)}"
            
            start_date = config.get("start_date")
            end_date = config.get("end_date")
            
            if start_date:
                try:
                    datetime.strptime(start_date, DATE_FORMAT)
                except ValueError:
                    return False, f"Invalid start_date format: {start_date}. Expected format: YYYY-MM-DD"
            
            if end_date:
                try:
                    datetime.strptime(end_date, DATE_FORMAT)
                except ValueError:
                    return False, f"Invalid end_date format: {end_date}. Expected format: YYYY-MM-DD"
                
            if start_date and end_date:
                if datetime.strptime(start_date, DATE_FORMAT) > datetime.strptime(end_date, DATE_FORMAT):
                    return False, f"start_date ({start_date}) cannot be later than end_date ({end_date})"
            
            # Create token to verify credentials are valid
            token = create_jwt_token(key_id, issuer_id, private_key)
            if not token:
                return False, "Failed to create JWT token with provided credentials"
            
            # Test the API connection using the provided report ID
            test_report_id = report_ids["app_install_performance"]
            
            # Test the API connection
            api_config = {"key_id": key_id, "issuer_id": issuer_id, "private_key": private_key}
            instances = get_report_instances(test_report_id, api_config)
            
            if instances and "data" in instances:
                return True, None
            else:
                return False, "Unable to fetch report instances. Please check your credentials."
                
        except Exception as e:
            return False, f"Error connecting to App Store Connect: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        report_ids = config["report_ids"]
        
        stream_configs = [
            {
                "name": "app_install_performance",
                "report_id": report_ids["app_install_performance"],
                "report_name": "App Install Performance"
            },
            {
                "name": "app_downloads_detailed",
                "report_id": report_ids["app_downloads_detailed"],
                "report_name": "App Downloads Detailed"
            },
            {
                "name": "app_installation_deletion_detailed",
                "report_id": report_ids["app_installation_deletion_detailed"],
                "report_name": "App Store Installation and Deletion Detailed"
            },
            {
                "name": "app_sessions_detailed", 
                "report_id": report_ids["app_sessions_detailed"],
                "report_name": "App Sessions Detailed"
            },
            {
                "name": "app_discovery_engagement_detailed",
                "report_id": report_ids["app_discovery_engagement_detailed"],
                "report_name": "App Store Discovery and Engagement Detailed"
            }
        ]
        
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
        
        self.temp_dir = tempfile.mkdtemp(prefix="app_store_")
        
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
            end_date = datetime.strptime(self.end_date, DATE_FORMAT)
            
        # Default to 3 days before end_date if start_date not provided
        if self.start_date:
            start_date = datetime.strptime(self.start_date, DATE_FORMAT)
        else:
            start_date = end_date - timedelta(days=3)

        # Generate daily slices
        slices = []
        current_date = start_date
        while current_date <= end_date:
            slices.append({"date": current_date.strftime(DATE_FORMAT)})
            current_date += timedelta(days=1)
            
        return slices
    
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Dict[str, Any]] = None,
        stream_state: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        date = stream_slice.get("date", datetime.now().strftime(DATE_FORMAT)) if stream_slice else datetime.now().strftime(DATE_FORMAT)
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