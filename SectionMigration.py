import boto3
import json
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# AWS Configuration
REGION = 'us-east-1'
SOURCE_TABLE_NAME = 'dev-languageApp-spanishSections'           # Adjust to actual source table name
TARGET_TABLE_NAME = 'juno-middleware-languageApp-ChatterBoxSections'  # Target table name

# Initialize AWS services
dynamodb = boto3.resource('dynamodb', region_name=REGION)
source_table = dynamodb.Table(SOURCE_TABLE_NAME)
target_table = dynamodb.Table(TARGET_TABLE_NAME)

# Create a standard deserializer from boto3
deserializer = TypeDeserializer()
# Set of DynamoDB types used as single-key wrappers
DYNAMO_TYPES = {"S", "N", "BOOL", "L", "M", "B", "SS", "NS", "BS"}

def custom_deserialize(value):
    """
    Recursively deserialize a value from DynamoDB JSON format.
    If a dict has exactly one key and that key is a known DynamoDB type,
    try to deserialize it. On failure, process recursively.
    """
    if isinstance(value, dict):
        if len(value) == 1:
            key = next(iter(value.keys()))
            if key in DYNAMO_TYPES:
                try:
                    return deserializer.deserialize(value)
                except Exception as e:
                    print("Error deserializing value:", value, e)
                    return {k: custom_deserialize(v) for k, v in value.items()}
            else:
                return {k: custom_deserialize(v) for k, v in value.items()}
        else:
            return {k: custom_deserialize(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [custom_deserialize(item) for item in value]
    else:
        return value

def deserialize_item(raw_item):
    """Deserialize a raw DynamoDB item into a normal Python dict."""
    return {key: custom_deserialize(val) for key, val in raw_item.items()}

def fix_lesson(lesson):
    """
    Remove the ImageInfo field from a lesson unless the lesson is of type PhotoList
    and has non-empty ImageObjects.
    """
    if not isinstance(lesson, dict):
        return lesson

    lesson_type = lesson.get("Type", "")
    if lesson_type != "PhotoList":
        lesson.pop("ImageInfo", None)
    else:
        # For PhotoList lessons, check if ImageInfo exists
        image_info = lesson.get("ImageInfo", {})
        image_objects = image_info.get("ImageObjects", None)
        # If ImageObjects is a string equal to "[]" or an empty list, remove ImageInfo.
        if (isinstance(image_objects, str) and image_objects.strip() == "[]") or \
           (isinstance(image_objects, list) and len(image_objects) == 0):
            lesson.pop("ImageInfo", None)
    return lesson

def transform_lessons(lessons):
    """
    Process the lessons list by applying fix_lesson to each lesson,
    then return the result as a JSON string.
    """
    if not isinstance(lessons, list):
        return json.dumps([])

    fixed_lessons = []
    for lesson in lessons:
        if isinstance(lesson, dict):
            fixed_lessons.append(fix_lesson(lesson))
    return json.dumps(fixed_lessons, ensure_ascii=False)

def transform_item(item):
    """
    Build the new target item.
    We assume each source item already has a top-level Lessons array.
    """
    new_item = {
        "Identifier": item.get("Identifier", "Unknown"),
        "Targ_Lang_Code": "ES",      # Set target language to Spanish
        "Base_Lang_Code": "EN",  # Set base language to English
        "Lessons": transform_lessons(item.get("Lessons", []))
    }
    return new_item

def migrate_items():
    try:
        print("Starting sections migration...")
        all_items = []
        response = source_table.scan()
        all_items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = source_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            all_items.extend(response.get("Items", []))
        print(f"Found {len(all_items)} items in the source table.\n")

        # Deserialize all items
        deserialized_items = [deserialize_item(raw) for raw in all_items]
        
        # Optional: print one deserialized item for debugging
        if deserialized_items:
            print("-- Example deserialized item --")
            print(json.dumps(deserialized_items[0], indent=2, ensure_ascii=False))
        
        # Transform and write each item to the target table.
        with target_table.batch_writer() as batch:
            for idx, item in enumerate(deserialized_items):
                new_item = transform_item(item)
                batch.put_item(Item=new_item)
                print(f"Migrated {idx+1}/{len(deserialized_items)}: {new_item['Identifier']}")
        
        print("\nMigration completed successfully.")
    
    except ClientError as e:
        print("An error occurred:", e.response["Error"]["Message"])

if __name__ == "__main__":
    migrate_items()