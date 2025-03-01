import boto3
import json
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# AWS Configuration – adjust as needed
REGION = 'us-east-1'
SOURCE_TABLE_NAME = 'dev-languageApp-userActions'  # Replace with the name of your old table
TARGET_TABLE_NAME = 'userActions'       # New table name

# Initialize DynamoDB resources
dynamodb = boto3.resource('dynamodb', region_name=REGION)
source_table = dynamodb.Table(SOURCE_TABLE_NAME)
target_table = dynamodb.Table(TARGET_TABLE_NAME)

# Standard deserializer from boto3
deserializer = TypeDeserializer()
DYNAMO_TYPES = {"S", "N", "BOOL", "L", "M", "B", "SS", "NS", "BS"}

def custom_deserialize(value):
    """
    Recursively deserialize a value from DynamoDB JSON format.
    If a dict has exactly one key and that key is one of the known DynamoDB types,
    use the boto3 deserializer. Otherwise, process recursively.
    """
    if isinstance(value, dict):
        if len(value) == 1 and list(value.keys())[0] in DYNAMO_TYPES:
            return deserializer.deserialize(value)
        else:
            return {k: custom_deserialize(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [custom_deserialize(item) for item in value]
    else:
        return value

def deserialize_item(raw_item):
    """Convert a raw DynamoDB item (with type wrappers) into a plain Python dict."""
    return {key: custom_deserialize(val) for key, val in raw_item.items()}

def transform_item(item):
    """
    Transform the old user action item to the new schema.
    - Copies over user_id, event, device_id, event_detail, event_detail_2, event_detail_3,
      event_type, location_id, section, section_level, session_id, and timestamp.
    - Ignores the old event_id field.
    """
    new_item = {
        "user_id": item.get("user_id", ""),
        "event": item.get("event", ""),
        "device_id": item.get("device_id", ""),
        "event_detail": item.get("event_detail", ""),
        "event_detail_2": item.get("event_detail_2", ""),
        "event_detail_3": item.get("event_detail_3", ""),
        "event_type": item.get("event_type", ""),
        "location_id": item.get("location_id", ""),
        "section": item.get("section", ""),
        "section_level": item.get("section_level", ""),
        "session_id": item.get("session_id", ""),
        "timestamp": item.get("timestamp", "")
    }
    return new_item

def migrate_items():
    try:
        print("Starting user actions migration...")
        all_items = []
        response = source_table.scan()
        all_items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = source_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            all_items.extend(response.get("Items", []))
        print(f"Found {len(all_items)} items in the source table.\n")
        
        # Deserialize items
        deserialized_items = [deserialize_item(raw) for raw in all_items]
        
        # Optional: Print one deserialized item for debugging.
        if deserialized_items:
            print("-- Example deserialized item --")
            print(json.dumps(deserialized_items[0], indent=2))
        
        # Transform and write items to the target table.
        with target_table.batch_writer() as batch:
            for idx, item in enumerate(deserialized_items):
                new_item = transform_item(item)
                batch.put_item(Item=new_item)
                print(f"Migrated {idx+1}/{len(deserialized_items)}: user_id={new_item.get('user_id')}, event={new_item.get('event')}")
        
        print("\nMigration completed successfully.")
    
    except ClientError as e:
        print("An error occurred:", e.response["Error"]["Message"])

if __name__ == "__main__":
    migrate_items()
