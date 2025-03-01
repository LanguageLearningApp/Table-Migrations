import boto3
import json
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# AWS Configuration
REGION = 'us-east-1'
SOURCE_TABLE_NAME = 'dev-languageApp-spanishCourse'           # Adjust the source table name
TARGET_TABLE_NAME = 'juno-middleware-languageApp-ChatterBoxCourses'  # The target table

# Initialize AWS services
dynamodb = boto3.resource('dynamodb', region_name=REGION)
translate = boto3.client('translate', region_name=REGION)
source_table = dynamodb.Table(SOURCE_TABLE_NAME)
target_table = dynamodb.Table(TARGET_TABLE_NAME)

# Deserializer for DynamoDB JSON format
deserializer = TypeDeserializer()

def deserialize_item(item):
    """Convert a DynamoDB item with type wrappers into a plain Python dict."""
    return {k: deserializer.deserialize(v) for k, v in item.items()}

def translate_text(text, source_lang="en", target_lang="es"):
    """Translate text using AWS Translate."""
    if not text.strip():
        return ""
    
    try:
        response = translate.translate_text(
            Text=text,
            SourceLanguageCode=source_lang,
            TargetLanguageCode=target_lang
        )
        return response["TranslatedText"]
    except ClientError as e:
        print(f"Translation error: {e.response['Error']['Message']}")
        return text  # Fallback to original text if translation fails

def transform_item(item):
    """Transform a deserialized source item into the target schema."""
    
    # Extract and format the Images field (store as a JSON string)
    images = []
    if isinstance(item.get("Images"), list):
        images = [{"URL": img.get("URL", "")} for img in item["Images"]]
    
    images_json = json.dumps(images)  # Convert list to JSON string for DynamoDB storage

    # Extract and translate the description
    english_description = item.get("Description", "")
    translated_description = translate_text(english_description, source_lang="en", target_lang="es")

    new_item = {
        "Identifier": item.get("Identifier", ""),
        "Targ_Lang_Code": "ES",  # Target Language is always Spanish
        "Base_Lang_Code": "EN",  # Base Language is always English
        "City": item.get("City", ""),
        "Country": item.get("Country", ""),
        "Base_Lang_Description": translated_description,  # Spanish translation
        "Target_Lang_Description": english_description,  # Original English description
        "Images": images_json,  # Now storing as a JSON string
        "Vocabulary_List": item.get("Vocabulary_List", "")
    }
    return new_item

def migrate_items():
    try:
        print("Starting migration for Courses...")
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
                print(f"Migrated item Identifier: {new_item.get('Identifier')}, City: {new_item.get('City')}")
                
        print("Migration completed successfully.")
    
    except ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")

if __name__ == '__main__':
    migrate_items()
