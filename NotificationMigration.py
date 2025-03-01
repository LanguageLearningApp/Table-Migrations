import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# Configuration – update these values as needed
REGION = 'us-east-1'
SOURCE_TABLE_NAME = 'dev-languageApp-Notificationsv2'    # Adjust the source table name if necessary
TARGET_TABLE_NAME = 'juno-middleware-languageApp-Notificationsv3'  # The target table

# Initialize DynamoDB resource and tables
dynamodb = boto3.resource('dynamodb', region_name=REGION)
source_table = dynamodb.Table(SOURCE_TABLE_NAME)
target_table = dynamodb.Table(TARGET_TABLE_NAME)

# Deserializer to convert raw DynamoDB JSON (with "S", etc.) to Python types
deserializer = TypeDeserializer()

def deserialize_item(item):
    """
    Convert a DynamoDB item with type wrappers into a plain Python dict.
    """
    return {k: deserializer.deserialize(v) for k, v in item.items()}

def transform_item(item):
    """
    Transform a deserialized source item into the target item schema.
    Only the following fields are migrated:
      - Identifier
      - Language
      - Type
      - Title
      - Body
      - Date_Started
      - Date_Retired
      - isActive
      - Use_Case
    """
    new_item = {
        "Identifier": item.get("Identifier", ""),
        "Language": item.get("Language", ""),
        "Type": item.get("Type", ""),
        "Title": item.get("Title", ""),
        "Body": item.get("Body", ""),
        "Date_Started": item.get("Date_Started", ""),
        "Date_Retired": item.get("Date_Retired", ""),
        "isActive": item.get("isActive", ""),
        "Use_Case": item.get("Use_Case", ""),
        "Lang_Code": "EN"
    }
    return new_item

def migrate_items():
    try:
        print("Starting migration for Notifications...")
        response = source_table.scan()
        items = response.get('Items', [])
        
        # If items are in raw DynamoDB format (with "S", etc.), deserialize them
        if items and isinstance(next(iter(items[0].values())), dict) and 'S' in next(iter(items[0].values())):
            print("Deserializing items from raw DynamoDB format...")
            items = [deserialize_item(item) for item in items]
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = source_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            new_items = response.get('Items', [])
            if new_items and isinstance(next(iter(new_items[0].values())), dict) and 'S' in next(iter(new_items[0].values())):
                new_items = [deserialize_item(item) for item in new_items]
            items.extend(new_items)
        
        print(f"Found {len(items)} items in the source table.")
        
        # Write to target table using batch_writer for efficiency
        with target_table.batch_writer() as batch:
            for item in items:
                new_item = transform_item(item)
                batch.put_item(Item=new_item)
                print(f"Migrated item Identifier: {new_item.get('Identifier')}, Language: {new_item.get('Language')}")
                
        print("Migration completed successfully.")
    
    except ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")

if __name__ == '__main__':
    migrate_items()
