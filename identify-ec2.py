import os
import sys
import json
import boto3
import fnmatch
from tabulate import tabulate

#Extract temporary credentials and account ID from command-line arguments
access_key_id = sys.argv[1]
secret_access_key = sys.argv[2]
session_token = sys.argv[3]
target_account_id = sys.argv[4]
regions = ["us-east-1", "us-east-2", "us-west-1", "us-west-2"]

def is_protected(instance):
    #Convert instance tags into a dictionary
    tags = {tag['Key']: tag['Value'] for tag in instance.tags or []}
    #Check if at least a number of specific EKS-related tags are present
    eks_tags = ['kubernetes.io/cluster/*', 'aws:eks:cluster-name', 'eks:cluster-name', 'eks:nodegroup-name']
    tag_count = sum(1 for tag in tags if any(fnmatch.fnmatch(tag, pattern) for pattern in eks_tags))
    if tag_count >= 2:
        return True
    #Check if at least a number of specific EMR-related tags are present
    emr_tags = ['aws:elasticmapreduce:instance-group-role', 'aws:elasticmapreduce:job-flow-id']
    tag_count = sum(1 for tag in emr_tags if tag in tags)
    if tag_count >= 1:
        return True

    schedule_tag = tags.get('SCHEDULE')
    if schedule_tag == 'DO_NOT_STOP': 
        return True  #Skip instance if SCHEDULE is DO_NOT_STOP
    elif schedule_tag in ['PST', 'IST']:
        return False  #Eligible if SCHEDULE is PST or IST
    #Default: instance is eligible if no protection criteria are met
    return False

def detect_eligible_instances(ec2, region):
    eligible_instances = []
    for instance in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'stopped']}]):
        if not is_protected(instance):
            #Skip instances with SCHEDULE=DO_NOT_TOUCH
            schedule_tag = next((tag['Value'] for tag in instance.tags if tag['Key'] == 'SCHEDULE'), 'Not Set')
            if schedule_tag == 'DO_NOT_TOUCH':
                continue

            instance_info = {
                'InstanceId': instance.id,
                'InstanceName': next((tag['Value'] for tag in instance.tags if tag['Key'] == 'Name'), 'Unknown'),
                'State': instance.state['Name'],
                'Schedule': schedule_tag,
                'Region': region,
                'AccountId': target_account_id
            }
            eligible_instances.append(instance_info)
    return eligible_instances

def append_data_to_json(data, output_file):
    if os.path.exists(output_file):
        with open(output_file, 'r+') as json_file:
            existing_data = json.load(json_file)
            existing_data.extend(data)
            json_file.seek(0)
            json.dump(existing_data, json_file, indent=4)
    else:
        with open(output_file, 'w') as json_file:
            json.dump(data, json_file, indent=4)

def sort_and_print_table(header, output_file, sort_key):
    with open(output_file, 'r') as json_file:
        data = json.load(json_file)
    if not data:
        print(f"\nWARN: No instances to manage for account {target_account_id}.")
        print("--------------------------------------------------\n")
        return
    sorted_data = sorted([d for d in data if d['AccountId'] == target_account_id], key=lambda x: x[sort_key])
    print(tabulate([header] + [[item[key] for key in header] for item in sorted_data], headers="firstrow", tablefmt="grid"))

if __name__ == "__main__":
    header = ["InstanceId", "InstanceName", "State", "Schedule", "Region", "AccountId"]
    all_instances_file = "eligible-ec2.json"
    
    if not os.path.exists(all_instances_file):
        with open(all_instances_file, 'w') as json_file:
            json.dump([], json_file, indent=4)
            
    #Process each region and accumulate eligible instances
    for region in regions:
        print(f"\nINFO: Processing region '{region}'")
        ec2 = boto3.resource('ec2', region_name=region, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, aws_session_token=session_token)
        eligible_instances = detect_eligible_instances(ec2, region)
        append_data_to_json(eligible_instances, all_instances_file)
        print("--------------------------------------------------\n")

    #Sort and display only data for the specific account
    sort_and_print_table(header, all_instances_file, "InstanceId")

    print(f"INFO: Eligible instances saved to '{all_instances_file}' for account {target_account_id}")
    print("--------------------------------------------------\n")
