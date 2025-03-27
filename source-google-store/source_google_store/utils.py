"""
Utility functions for Google Play Store source connector.
"""
import csv
import logging
from datetime import datetime
from typing import Dict, List, Any, Mapping
from io import BytesIO, TextIOWrapper

def clean_report(content: bytes, package_name: str, report_path: str, stats_type: str = None) -> List[Dict[str, Any]]:
    """
    Parse and clean CSV report data from Google Play Store.
    Returns: A list of dictionaries with the cleaned report data.
    """
    try:
        binary_stream = BytesIO(content)
        text_stream = TextIOWrapper(binary_stream, encoding='utf-16')
        
        csv_reader = csv.reader(text_stream)
        headers = next(csv_reader)
        if headers and headers[0].startswith('\ufeff'):
            headers[0] = headers[0].replace('\ufeff', '')
        
        json_data = []
        for row in csv_reader:
            record = {}
            for i, header in enumerate(headers):
                clean_header = header.strip().strip('\x00')
                value = row[i].strip().strip('\x00') if i < len(row) else ""
                try:
                    if value.replace('.', '', 1).isdigit() or (value.startswith('-') and value[1:].replace('.', '', 1).isdigit()):
                        value = float(value)
                except (ValueError, TypeError):
                    pass
                
                record[clean_header] = value
            
            metadata = {
                'package_name': package_name,
                'report_path': report_path,
                'extracted_at': datetime.now().isoformat()
            }
            
            if stats_type:
                metadata['stats_type'] = stats_type
            
            record['_metadata'] = metadata
            
            json_data.append(record)
        
        return json_data
        
    except Exception as e:
        logging.error(f"Error processing report {report_path}: {str(e)}")
        return [] 