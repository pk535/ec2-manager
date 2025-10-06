import os
import sys
import json
import boto3
from tabulate import tabulate
from datetime import datetime

#Function to load instances and information from JSON file
def load_instances_from_json(file_path):
    try:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            instances = []
            for instance in data:
                instance_id = instance.get('InstanceId', 'Unknown')
                instance_name = instance.get('InstanceName', 'Unknown')
                region = instance.get('Region', 'Unknown') 
                schedule = instance.get('Schedule', 'Not Set')
                account_id = instance.get('AccountId', None)  #AccountId is required
                instances.append({'InstanceId': instance_id, 'InstanceName': instance_name, 
                                  'Region': region, 'Schedule': schedule, 'AccountId': account_id})
            return instances
    except Exception as e:
        print(f'ERROR: Unable to load instances from JSON file: {e}')
        print("--------------------------------------------------")
        sys.exit(1)
    else:
	    nmgjdhfdngvcnbvcnbcjd

#Function to find an instance by either InstanceId or InstanceName
def find_instance_by_identifier(identifier, instances):
    for instance in instances:
        if instance['InstanceId'] == identifier or instance['InstanceName'].lower() == identifier.lower():
            return instance
    return None

#Function to log the action to the processed-ec2.json
def log_action(instance, action):
    log_entry = {
        'InstanceId': instance['InstanceId'],
        'InstanceName': instance['InstanceName'],
        'Action': action,
        'Schedule': instance['Schedule'],
        'Region': instance['Region'],
        'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    try:
        if os.path.exists(processed_json_path):
            with open(processed_json_path, 'r') as f:
                processed_data = json.load(f)
        else:
            processed_data = []
    except Exception as e:
        print(f"ERROR: Failed to read processed-ec2.json: {e}")
        print("--------------------------------------------------")
        processed_data = []

    processed_data.append(log_entry)

    try:
        with open(processed_json_path, 'w') as f:
            json.dump(processed_data, f, indent=4)
    except Exception as e:
        print(f"ERROR: Failed to write to processed-ec2.json: {e}")
        print("--------------------------------------------------")

#Function to start an instance
def start_instance(instance):
    try:
        ec2 = boto3.resource('ec2', region_name=instance["Region"], aws_access_key_id=access_key_id,
                             aws_secret_access_key=secret_access_key, aws_session_token=session_token)
        ec2_instance = ec2.Instance(instance['InstanceId'])
        if ec2_instance.state['Name'] == 'stopped':
            print(f'INFO: Starting instance {instance["InstanceId"]} ({instance["InstanceName"]}) in {instance["Region"]}.')
            ec2_instance.start()
            ec2_instance.wait_until_running()
            print(f'SUCCESS: Instance {instance["InstanceId"]} ({instance["InstanceName"]}) is now running in {instance["Region"]}.')
            print("--------------------------------------------------")
            log_action(instance, 'Started')
        else:
            print(f'WARN: Instance {instance["InstanceId"]} ({instance["InstanceName"]}) is already running in {instance["Region"]}.')
            print("--------------------------------------------------")
    except Exception as e:
        print(f'ERROR: Error starting instance: {e}')
        print("--------------------------------------------------")

#Function to stop an instance
def stop_instance(instance):
    try:
        ec2 = boto3.resource('ec2', region_name=instance["Region"], aws_access_key_id=access_key_id,
                             aws_secret_access_key=secret_access_key, aws_session_token=session_token)
        ec2_instance = ec2.Instance(instance['InstanceId'])
        if ec2_instance.state['Name'] == 'running':
            print(f'INFO: Stopping instance {instance["InstanceId"]} ({instance["InstanceName"]}) in {instance["Region"]}.')
            ec2_instance.stop()
            ec2_instance.wait_until_stopped()
            print(f'SUCCESS: Instance {instance["InstanceId"]} ({instance["InstanceName"]}) is now stopped in {instance["Region"]}.')
            print("--------------------------------------------------")
            log_action(instance, 'Stopped')
        else:
            print(f'WARN: Instance {instance["InstanceId"]} ({instance["InstanceName"]}) is already stopped in {instance["Region"]}.')
            print("--------------------------------------------------")
    except Exception as e:
        print(f'ERROR: Error stopping instance: {e}')
        print("--------------------------------------------------")

def stop_all_with_schedule(target_schedule, current_account_id):
    try:
        instances = load_instances_from_json(json_file_path)
        if not instances:
            print("No instances found in the JSON file.")
            print("--------------------------------------------------")
            return
        
        #Filter instances for the current account
        account_instances = [inst for inst in instances if inst.get('AccountId') == current_account_id]
        if not account_instances:
            print(f"Skipping account {current_account_id} because no instance is available for action.")
            print("--------------------------------------------------")
            return
        
        print(f"INFO: Stopping instances from the JSON file with SCHEDULE={target_schedule} for Account ID={current_account_id}")
        
        for instance in account_instances:
            try:
                ec2 = boto3.resource('ec2', region_name=instance["Region"], aws_access_key_id=access_key_id,
                                     aws_secret_access_key=secret_access_key, aws_session_token=session_token)
                ec2_instance = ec2.Instance(instance['InstanceId'])

                #Validate instance existence
                ec2_instance.load()
                schedule_tag = next((tag['Value'] for tag in ec2_instance.tags if tag['Key'] == 'SCHEDULE'), 'IST')
                if schedule_tag.upper() == target_schedule.upper():
                    stop_instance(instance)
                else:
                    print(f"WARN: Skipping instance {instance['InstanceId']} ({instance['InstanceName']}), SCHEDULE={schedule_tag}")
                    print("--------------------------------------------------")
            except ec2.meta.client.exceptions.ClientError as e:
                if 'InvalidInstanceID.NotFound' in str(e):
                    print(f"WARN: Instance {instance['InstanceId']} does not exist in account {current_account_id}")
                    print("--------------------------------------------------")
                else:
                    print(f"ERROR: Unexpected error for instance {instance['InstanceId']}: {e}")
                    print("--------------------------------------------------")
    except Exception as e:
        print(f'ERROR: Error stopping instances by schedule for Account ID={current_account_id}: {e}')
        print("--------------------------------------------------")


if __name__ == "__main__":
    #Set credentials
    access_key_id = sys.argv[1]
    secret_access_key = sys.argv[2]
    session_token = sys.argv[3]
    passed_account_id = sys.argv[4]  #Account ID passed in the argument

    action = os.environ['ACTION']
    instance_identifier = os.environ['INSTANCE']
    target_schedule = os.environ['TARGET_SCHEDULE']
    json_file_path = "eligible-ec2.json"
    processed_json_path = "processed-ec2.json"

    print("**************************************************")
    print(f"** Action   : {action}")
    print(f"** Instance : {instance_identifier}")
    print(f"** Account ID : {passed_account_id}")
    print("**************************************************\n")

    #Load instances from the JSON file
    instances = load_instances_from_json(json_file_path)

    if action == 'start':
        #Start a specific instance
        instance = find_instance_by_identifier(instance_identifier, instances)
        if instance:
            start_instance(instance)
        else:
            print(f"ERROR: Instance {instance_identifier} not found in JSON file.")
            print("--------------------------------------------------")

    elif action == 'stop':
        if instance_identifier.lower() == 'all':
            if target_schedule:
                stop_all_with_schedule(target_schedule, passed_account_id)
            else:
                print("ERROR: For 'stop all', a valid TARGET_SCHEDULE is required.")
                print("--------------------------------------------------")
        else:
            #Stop a specific instance
            instance = find_instance_by_identifier(instance_identifier, instances)
            if instance:
                stop_instance(instance)
            else:
                print(f"ERROR: Instance {instance_identifier} not found in JSON file.")
                print("--------------------------------------------------")

    else:
        print(f"ERROR: Invalid action '{action}'. Use 'start', 'stop', or 'stop all'.")
        print("--------------------------------------------------")

