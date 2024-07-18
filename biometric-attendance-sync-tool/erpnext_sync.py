import local_config as config
import requests
import datetime
import json
import os
import time
import logging
import pickledb
from datetime import datetime, timedelta

EMPLOYEE_NOT_FOUND_ERROR_MESSAGE = "No Employee found for the given employee field value"
EMPLOYEE_INACTIVE_ERROR_MESSAGE = "Transactions cannot be created for an Inactive Employee"
DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE = "This employee already has a log with the same timestamp"
ALLOWLISTED_ERRORS = [
    EMPLOYEE_NOT_FOUND_ERROR_MESSAGE,
    EMPLOYEE_INACTIVE_ERROR_MESSAGE,
    DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE,
]

# Load allowlisted errors from config if available
if hasattr(config, 'allowed_exceptions'):
    ALLOWLISTED_ERRORS = [ALLOWLISTED_ERRORS[i-1] for i in config.allowed_exceptions]

# Default configurations
ERPNEXT_VERSION = getattr(config, 'ERPNEXT_VERSION', 14)
LOGS_DIRECTORY = "./logs"
if not os.path.exists(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY)

def setup_logger(name, log_file, level=logging.INFO, formatter=None):
    if not formatter:
        formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
    
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    
    return logger

error_logger = setup_logger('error_logger', os.path.join(LOGS_DIRECTORY, 'error.log'), logging.ERROR)
info_logger = setup_logger('info_logger', os.path.join(LOGS_DIRECTORY, 'logs.log'))

status = pickledb.load(os.path.join(LOGS_DIRECTORY, 'status.json'), True)
processed_entries = pickledb.load(os.path.join(LOGS_DIRECTORY, 'processed_entries.json'), True)

def _safe_convert_date(datestring, pattern):
    try:
        if datestring is None:
            raise ValueError("Date string is None")
        
        converted_date = datetime.strptime(datestring, pattern)
        formatted_date = converted_date.strftime("%Y-%m-%d %H:%M:%S")
        
        info_logger.info(f"Successfully converted date string: {datestring} to {formatted_date}")
        return converted_date
    except ValueError as ve:
        raise ve
    except Exception as e:
        error_logger.exception(f"Failed to convert date string: {datestring}, pattern: {pattern}, error: {e}")
        return None

def _safe_get_error_str(res):
    try:
        error_json = json.loads(res.content.decode('utf-8'))
        if 'exc' in error_json:
            exc_message = json.loads(error_json['exc'])[0]
            return exc_message.split(": ")[-1].strip()
        elif '_server_messages' in error_json:
            return error_json['_server_messages'][0]['message']
        return json.dumps(error_json)
    except Exception as e:
        error_logger.exception(f"Failed to get error string from response: {res}, error: {e}")
        return str(res.content)

def fetch_data_from_api():
    try:
        start_time = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S%z')
        end_time = datetime.now().replace(hour=23, minute=59, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S%z')

        #url for taking live sync data from biometric device
        url = "https://127.0.0.1:443/artemis/api/acs/v1/door/events"
        
        #headers and payload for hikvision machine which is given by hikvsion vendor
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json;charset=UTF-8',
            'X-Ca-Key': '21748897',
            'X-Ca-Signature': '0ypjUr31ivbKx/Oe4fgeAJIXoLxnuRNp3Xer6znTAC4='
        }
        
        page_no = 1
        total_records = 0
        attendances = []

        while True:
            payload = json.dumps({
                # "startTime": start_time + "+05:30",
                # "endTime": "2024-07-17T23:59:00+05:30",
                "startTime": "2024-07-01T00:00:00+05:30",
                "endTime": end_time + "+05:30",
                "eventType": 196893,
                "doorIndexCodes": ["1"],
                "pageNo": page_no,
                "pageSize": 500,
                "temperatureStatus": -1,
                "maskStatus": -1,
                "orderType": 1
            })
            
            response = requests.post(url, headers=headers, data=payload, verify=False)
            response.raise_for_status()
        
            data = response.json()
            info_logger.info(f"Raw data fetched from API: {data}")

            records = data.get('data', {}).get('list', [])
            total_records += len(records)
            if not records:
                break

            for record in records:
                employee_field_value = record['personId']
                device_time = record['deviceTime']
                datestring = device_time
                info_logger.info(f"Processing time: {datestring}")
                converted_date = _safe_convert_date(datestring, "%Y-%m-%dT%H:%M:%S%z")
                if converted_date is not None:
                    time_part = converted_date.strftime("%H:%M:%S%z")
                    log_type = "IN" if time_part <= "13:30:00+0530" else "OUT"
                    attendances.append({'employee_field_value': employee_field_value, 'timestamp': converted_date, 'log_type': log_type})
            
            page_no += 1
        
        info_logger.info(f"Total records fetched: {total_records}")
        return attendances
    except requests.exceptions.RequestException as e:
        error_logger.exception(f"HTTP Request failed: {e}")
        return []
    except Exception as e:
        error_logger.exception(f"Failed to fetch or process data from API: {e}")
        return []

def send_to_erpnext(employee_field_value, timestamp, log_type=None):
    if not isinstance(timestamp, datetime):
        error_logger.error(f"Timestamp is not a datetime object: {timestamp}")
        return 400, "Invalid timestamp"
    
    formatted_time = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    
    endpoint_app = "hrms" if ERPNEXT_VERSION > 13 else "erpnext"
    url = f"{config.ERPNEXT_URL}/api/method/{endpoint_app}.hr.doctype.employee_checkin.employee_checkin.add_log_based_on_employee_field"
    headers = {
        'Authorization': f"token {config.ERPNEXT_API_KEY}:{config.ERPNEXT_API_SECRET}",
        'Accept': 'application/json'
    }
    data = {
        'employee_field_value': employee_field_value,
        'timestamp': formatted_time,
        'log_type': log_type,
        'device_id': 'hikvision',
    }
    
    info_logger.info(f"Sending request to ERPNext: {data}")
    
    response = requests.post(url, headers=headers, json=data)
    
    info_logger.info(f"ERPNext response status: {response.status_code}, content: {response.content}")
    
    if response.status_code == 200:
        return 200, json.loads(response.content)['message']['name']
    else:
        error_str = _safe_get_error_str(response)
        error_logger.error(f"Error during ERPNext API Call. Status Code: {response.status_code}, Response: {response.content}, Request Data: {data}")
        return response.status_code, error_str

def main():
    info_logger.info('Fetching data from API.')
    device_attendance_logs = fetch_data_from_api()
    print(f"Number of logs to process: {len(device_attendance_logs)}")
    for log in device_attendance_logs:
        timestamp_str = log['timestamp'].isoformat()
        info_logger.info(f"Processing log: {log}")
        erpnext_status_code, erpnext_message = send_to_erpnext(
            log['employee_field_value'], log['timestamp'], log.get('log_type')
        )
        if erpnext_status_code == 200:
            processed_entries.set(timestamp_str, True)
            processed_entries.dump()
        elif erpnext_status_code != 200:
            error_logger.error(f"Failed to process log: {log}, Error: {erpnext_message}")
            continue

def infinite_loop(sleep_time=15):
    print("Service Running...")
    while True:
        try:
            main()
            time.sleep(sleep_time)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    infinite_loop()
