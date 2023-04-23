import boto3
import os
import pandas
import botocore.exceptions

region='us-east-1'
ec2_client = boto3.client('ec2',region_name=region)
s3_client = boto3.client('s3',region_name=region)

def create_key_pair():
    key_pair = ec2_client.create_key_pair(KeyName="ec2-key-pair")
    private_key = key_pair["KeyMaterial"]
    with os.fdopen(os.open("aws_ec2_key.pem", os.O_WRONLY | os.O_CREAT, 0o400), "w+") as handle:
        handle.write(private_key)

def create_instance():
    instances = ec2_client.run_instances(
        ImageId="ami-007855ac798b5175e",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName="ec2-key-pair"
    )
    print(instances["Instances"][0]["InstanceId"])
 
def get_running_instance():
    reservations = ec2_client.describe_instances(Filters=[
        {
            "Name": "instance-state-name",
            "Values": ["running"],
        },
        {
            "Name": "instance-type",
            "Values": ["t2.micro"]
        }
    ]).get("Reservations")
    ls = []
    for reservation in reservations:
        for instance in reservation["Instances"]:
            ls.append(instance["InstanceId"])
    return ls

def get_ip(instance_id):
    inst=get_running_instance()
    if instance_id in inst:
        reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get("Reservations")
        #print(reservations[0]['Instances'][0]['PublicIpAddress'])
        return(reservations[0]['Instances'][0]['PublicIpAddress'])
    else:
        print("The instance is not running or non-existent yet. No IP can be gathered.\n")

#get_ip('i-0c5c138784e7c849b')

def ssh():
    tmp = get_ip('i-0c5c138784e7c849b')
    print(f"command ssh: ssh -i aws_ec2_key.pem ec2-user@{tmp}")

#ssh()

#видалення істансу
def terminate_instance(instance_id):
    response = ec2_client.terminate_instances(InstanceIds=[instance_id])
    return response

def stop_instance(instance_id):
    response = ec2_client.stop_instances(InstanceIds=[instance_id])
    return response

#stop_instance('i-0c5c138784e7c849b')

def start_instance(instance_id):
    response = ec2_client.start_instances(InstanceIds=[instance_id])
    return response

#start_instance('i-0c5c138784e7c849b')

def get_instance_info(instance_id):
    response = ec2_client.describe_instance_status(InstanceIds=[instance_id])
    print(response)

#get_instance_info('i-0c5c138784e7c849b')     

def bucket_exists(bucket_name):
    s3_cl = boto3.resource('s3')
    if s3_cl.Bucket(bucket_name) not in s3_cl.buckets.all():
        return False
    return True


def bucket_element_exists(bucket_name, s3_obj_name):
    try:
        s3_client.get_object(Bucket = bucket_name, Key = s3_obj_name)
    except:
        return False
    return True

#bucket_element_exists('tostoganlab', 'exchange.csv')

def create_bucket(bucket_name):
    try:
        response = s3_client.create_bucket(Bucket=bucket_name)
        print(response)
    except botocore.exceptions.ClientError:
        print("Error, such backet is already exists")
        return
    except botocore.exceptions.ParamValidationError:
        print("Error, invalid name. Bucket name must contain only letters, numbers and '-'")
        return

#create_bucket('janelab4')
    
def buckets_list():
    response = s3_client.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f' {bucket["Name"]}')
        

def upload(file_name, bucket_name, s3_obj_name):
    if not bucket_exists(bucket_name):
        print("Error. No such bucket")
        return
    if not os.path.exists(file_name):
        print(f"{file_name}:: such file does not exists")
        return
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_obj_name)
    except:
        response = s3_client.upload_file(Filename=file_name, Bucket=bucket_name, Key=s3_obj_name)
        print(response)
        return
    print(f"{s3_obj_name} is already exists on {bucket_name}")
    
def upload_file(filename, bucketname):
    if not bucket_exists(bucketname):
        print("Error. No such bucket")
        return
    try:
        s3_client.head_object(Bucket=bucketname, Key=filename)
    except:
        with open(filename, "rb") as f:
            response = s3_client.upload_fileobj(f, bucketname, filename)
        print(response)
        return
    print(f"{filename} is already exists on {bucketname}")
    

#upload_file('exchange.csv', 'janelab4')
    
#upload('/Users/User/Desktop/хмари/data.csv', 'janelab4', 'curr.csv')

def read_csv_from_bucket(bucket_name, s3_obj_name):
    if not bucket_exists(bucket_name):
        print(F"Error. No such bucket {bucket_name}")
        return
    if not bucket_element_exists(bucket_name, s3_obj_name):
        print(F"Error. No such file {s3_obj_name}")
        return
    obj = s3_client.get_object(
        Bucket=bucket_name,
        Key=s3_obj_name
    )
    data = pandas.read_csv(obj['Body'])
    print('Printing the data frame...')
    print(data.head())

#read_csv_from_bucket('janelab4', 'exchange.csv')

def destroy_bucket(bucket_name):
    response = s3_client.delete_bucket(Bucket=bucket_name)
    print(response)

#destroy_bucket('janelab4')
