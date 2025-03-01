import boto3
import json
import decimal
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# AWS Configuration – update these as needed
REGION = 'us-east-1'
OLD_TABLE_NAME = 'dev-languageApp-spanishUsers'  # Replace with your current table name
NEW_TABLE_NAME = 'juno-middleware-languageApp-ChatterBoxUsers'      # New table as defined in SST

# Initialize DynamoDB resources
dynamodb = boto3.resource('dynamodb', region_name=REGION)
old_table = dynamodb.Table(OLD_TABLE_NAME)
new_table = dynamodb.Table(NEW_TABLE_NAME)

# Standard deserializer from boto3
deserializer = TypeDeserializer()
DYNAMO_TYPES = {"S", "N", "BOOL", "L", "M", "B", "SS", "NS", "BS"}

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON Encoder that converts Decimal objects to strings."""
    def default(self, o):
        if isinstance(o, (decimal.Decimal, int, float)):
            return str(o)
        return super(DecimalEncoder, self).default(o)

def convert_to_string(val):
    """Convert numeric values (int, float, Decimal) to string; otherwise return the value."""
    if isinstance(val, (int, float, decimal.Decimal)):
        return str(val)
    return val

def custom_deserialize(value):
    """
    Recursively deserialize a value from DynamoDB JSON format.
    If it's a dict with exactly one key and that key is in DYNAMO_TYPES, try deserializing it.
    """
    if isinstance(value, dict):
        if len(value) == 1 and list(value.keys())[0] in DYNAMO_TYPES:
            try:
                return convert_to_string(deserializer.deserialize(value))
            except Exception as e:
                print("Error deserializing value:", value, e)
                return {k: custom_deserialize(v) for k, v in value.items()}
        else:
            return {k: custom_deserialize(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [custom_deserialize(item) for item in value]
    else:
        return convert_to_string(value)

def deserialize_item(raw_item):
    """Deserialize a raw DynamoDB item into a plain Python dict with numbers as strings."""
    return {key: custom_deserialize(val) for key, val in raw_item.items()}

def transform_item(item):
    """
    Transform an item from the old table to match the new schema.
    - Set Base_Lang to "EN".
    - Set Targ_Lang to "ES" with Current_Section and Current_Lesson as strings.
    - Remove the old top-level Current_Section and Current_Lesson.
    - Copy other fields unchanged, ensuring numbers are strings.
    """
    new_item = {}
    
    # Fields to copy unchanged
    fields_to_copy = [
        "Identifier", "Email", "Account_Creation_Date", "Birthday",
        "Commitment_Level", "Country", "DailyAvailability", "Device_Information",
        "FCM_Token", "First_Name", "Gender", "Preffered_Language", "Profile_Picture",
        "Role", "Streak", "Subscription_End_Date", "Subscription_Start_Date",
        "Subscription_Status", "Time_Zone", "Usage_Metrics", "User_Preferences",
        "User_subscription_experiation", "Last_Login", "Last_Name", "Last_Streak_Change",
        "Lives", "Location", "Motivations"
    ]
    for field in fields_to_copy:
        new_item[field] = convert_to_string(item.get(field, ""))
    
    # Hardcode Base_Lang to "EN"
    new_item["Base_Lang"] = "EN"
    
    # Hardcode Targ_Lang to "ES" with Current_Section and Current_Lesson
    current_section = convert_to_string(item.get("Current_Section", ""))
    current_lesson = convert_to_string(item.get("Current_Lesson", ""))
    
    targ_lang_details = {
        "ES": {
            "Current_Section": current_section,
            "Current_Lesson": current_lesson
        }
    }
    new_item["Targ_Lang"] = json.dumps(targ_lang_details, ensure_ascii=False, cls=DecimalEncoder)
    
    return new_item

def migrate_items():
    try:
        print("Starting users migration...")
        all_items = []
        response = old_table.scan()
        all_items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = old_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            all_items.extend(response.get("Items", []))
        print(f"Found {len(all_items)} items in the old table.\n")
        
        # Deserialize items
        deserialized_items = [deserialize_item(raw) for raw in all_items]
        
        # Print an example deserialized item for verification.
        if deserialized_items:
            print("-- Example deserialized item --")
            print(json.dumps(deserialized_items[0], indent=2, ensure_ascii=False, cls=DecimalEncoder))
        
        # Transform and write items to the new table.
        with new_table.batch_writer() as batch:
            for idx, item in enumerate(deserialized_items):
                new_item = transform_item(item)
                batch.put_item(Item=new_item)
                print(f"Migrated {idx+1}/{len(deserialized_items)}: {new_item.get('Identifier')}")
        
        print("\nMigration completed successfully.")
    
    except ClientError as e:
        print("An error occurred:", e.response["Error"]["Message"])

if __name__ == "__main__":
    migrate_items()