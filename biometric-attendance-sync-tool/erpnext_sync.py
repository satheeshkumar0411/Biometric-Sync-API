import local_config as config
import requests
import json
import os
import time
import logging
import pickledb
from datetime import datetime, timedelta

EMPLOYEE_NOT_FOUND_ERROR_MESSAGE = "No Employee found for the given employee field value"
EMPLOYEE_INACTIVE_ERROR_MESSAGE = "Transactions cannot be created for an Inactive Employee"
DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE = "This employee already has a log with the same"
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
        
        # info_logger.info(f"Successfully converted date string: {datestring} to {formatted_date}")
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
            return json.loads(error_json['_server_messages'])[0]['message']
        return json.dumps(error_json)
    except Exception as e:
        error_logger.exception(f"Failed to get error string from response: {res}, error: {e}")
        return str(res.content)

def log_error_to_frappe(error_message):
    url = f"{config.ERPNEXT_URL}/api/method/frappe.utils.error_log.create_new"
    headers = {
        'Authorization': f"token {config.ERPNEXT_API_KEY}:{config.ERPNEXT_API_SECRET}",
        'Accept': 'application/json'
    }
    data = {
        'title': 'Employee not found:',
        'error': error_message
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        error_logger.error(f"Failed to log error to Frappe. Status Code: {response.status_code}, Response: {response.content}")

def fetch_data_from_api():
    try:
        latest_fetched_time = status.get('latest_fetched_time')
        if latest_fetched_time:
            start_time = datetime.strptime(latest_fetched_time, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%dT%H:%M:%S') + "+05:30"
            # info_logger.info(f"Latest fetched time from status: {latest_fetched_time}")
        else:
            start_time = config.IMPORT_START_DATE
            # info_logger.info(f"No latest fetched time found. Using IMPORT_START_DATE: {config.IMPORT_START_DATE}")

        end_time = datetime.now().replace(hour=23, minute=59, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S%z')
        # info_logger.info(f"Fetching data from start_time: {start_time} to end_time: {end_time}")
        
        url = config.HIKVISION_API_URL
        
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json;charset=UTF-8',
            'X-Ca-Key': '21748897',
            'X-Ca-Signature': '0ypjUr31ivbKx/Oe4fgeAJIXoLxnuRNp3Xer6znTAC4='
        }
        
        page_no = 1
        total_records = 0
        attendances = []
        latest_fetched_time = None
        
        print("LAST FETCH TIME", start_time)
        
        while True:
            payload = json.dumps({
                "startTime": start_time,
                "endTime": end_time + "+05:30",
                "eventType": 196893,
                "doorIndexCodes": ["1"],
                "pageNo": page_no,
                "pageSize": 500,
                "temperatureStatus": -1,
                "maskStatus": -1,
                "orderType": 1
            })
            
            # info_logger.info(f"Sending request to API with payload: {payload}")

            response = requests.post(url, headers=headers, data=payload, verify=False)
            response.raise_for_status()
        
            data = response.json()
            # info_logger.info(f"Raw data fetched from API: {json.dumps(data)}")

            records = data.get('data', {}).get('list', [])
            total_records += len(records)
            if not records:
                # info_logger.info(f"No more records found in API response. Total records fetched: {total_records}")
                break

            for record in records:
                employee_field_value = record['personId']
                device_time = record['deviceTime']
                datestring = device_time
                info_logger.info(f"Processing record with time: {datestring} and employee_field_value: {employee_field_value}")
                converted_date = _safe_convert_date(datestring, "%Y-%m-%dT%H:%M:%S%z")
                if converted_date is not None:
                    if latest_fetched_time is None or converted_date > latest_fetched_time:
                        latest_fetched_time = converted_date

                    time_part = converted_date.strftime("%H:%M:%S%z")
                    log_type = "IN" if time_part <= "13:30:00+0530" else "OUT"
                    attendances.append({'employee_field_value': employee_field_value, 'timestamp': converted_date, 'log_type': log_type})
            
            page_no += 1

        attendances.sort(key=lambda x: x['timestamp'])
        
        # info_logger.info(f"Total records fetched: {total_records}")

        return attendances, latest_fetched_time
    except requests.exceptions.RequestException as e:
        error_logger.exception(f"HTTP Request failed: {e}")
        return [], None
    except Exception as e:
        error_logger.exception(f"Failed to fetch or process data from API: {e}")
        return [], None

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
    
    # info_logger.info(f"Sending request to ERPNext: {data}")
    
    response = requests.post(url, headers=headers, json=data)
    
    # info_logger.info(f"ERPNext response status: {response.status_code}, content: {response.content}")
    
    if response.status_code == 200:
        return 200, json.loads(response.content)['message']['name']
    else:
        error_str = _safe_get_error_str(response)
        
        if "This employee already has a log with the same timestamp" in error_str:
            # info_logger.info(f"Skipping log due to duplicate timestamp: {error_str}")
            return response.status_code, error_str
        
        if error_str not in ALLOWLISTED_ERRORS:
            error_logger.error(f"Error during ERPNext API Call. Status Code: {response.status_code}, Response: {response.content}, Request Data: {data}")
        
        if error_str in ALLOWLISTED_ERRORS:
            log_error_to_frappe(error_str)
        
        return response.status_code, error_str

def clear_error_log():
    open(os.path.join(LOGS_DIRECTORY, 'error.log'), 'w').close()
    # info_logger.info("Cleared error.log file.")

def clear_logs():
    open(os.path.join(LOGS_DIRECTORY, 'logs.log'), 'w').close()
    
def main():
    # info_logger.info('Fetching data from API.')
    device_attendance_logs, latest_fetched_time = fetch_data_from_api()
    print(f"Number of logs to process: {len(device_attendance_logs)}")
    for log in device_attendance_logs:
        timestamp_str = log['timestamp'].isoformat()
        # info_logger.info(f"Processing log: {log}")
        erpnext_status_code, erpnext_message = send_to_erpnext(
            log['employee_field_value'], log['timestamp'], log.get('log_type')
        )
        if erpnext_status_code == 200:
            processed_entries.set(timestamp_str, True)
            processed_entries.dump()
        elif erpnext_status_code != 200:
            error_logger.error(f"Failed to process log: {log}, Error: {erpnext_message}")
            continue

    if latest_fetched_time:
        new_start_time = (latest_fetched_time).strftime('%Y-%m-%dT%H:%M:%S%z')
        status.set('latest_fetched_time', new_start_time)
        status.dump()
        # info_logger.info(f"Updated latest_fetched_time to: {new_start_time}")

def infinite_loop(sleep_time=15):
    print("Service Running...")
    last_cleared = status.get('last_cleared')
    if not last_cleared:
        last_cleared = datetime.now()
        status.set('last_cleared', last_cleared.strftime('%Y-%m-%dT%H:%M:%S'))
        status.dump()
    else:
        last_cleared = datetime.strptime(last_cleared, '%Y-%m-%dT%H:%M:%S')
    
    while True:
        try:
            main()
            current_time = datetime.now()
            if (current_time - last_cleared) >= timedelta(days=30):
                clear_error_log()
                clear_logs()
                last_cleared = current_time
                status.set('last_cleared', last_cleared.strftime('%Y-%m-%dT%H:%M:%S'))
                status.dump()
            time.sleep(sleep_time)
        except Exception as e:
            print(e)
            
if __name__ == "__main__":
    infinite_loop()
