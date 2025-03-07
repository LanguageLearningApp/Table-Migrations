import boto3
import json
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# Configuration – update these values as needed
REGION = 'us-east-1'
SOURCE_TABLE_NAME = 'dev-languageApp-spanishVocab'
TARGET_TABLE_NAME = 'jared-data-languageApp-ChatterBoxVocab'

# Initialize DynamoDB resource and tables
dynamodb = boto3.resource('dynamodb', region_name=REGION)
source_table = dynamodb.Table(SOURCE_TABLE_NAME)
target_table = dynamodb.Table(TARGET_TABLE_NAME)

# Create a deserializer to convert raw DynamoDB JSON to Python types
deserializer = TypeDeserializer()

def deserialize_item(item):
    """
    Convert a DynamoDB item (with 'S', 'L', 'M' wrappers) into a plain Python dict.
    """
    return {k: deserializer.deserialize(v) for k, v in item.items()}

def transform_item(item):
    """
    Transform a deserialized item from the source schema into the target schema.
    """
    # Convert lists to JSON-encoded strings directly (no {"S": ...} wrapping here)
    english_options = json.dumps(item.get("EnglishOptions", [])) if isinstance(item.get("EnglishOptions"), list) else "[]"
    spanish_options = json.dumps(item.get("SpanishOptions", [])) if isinstance(item.get("SpanishOptions"), list) else "[]"
    
    # Extract syllables from the source data and JSON-encode as a list
    syllables_list = item.get("Syllables", []) if isinstance(item.get("Syllables"), list) else []
    # If syllables_list contains dictionaries (e.g., from raw DynamoDB format), extract the "S" values
    if syllables_list and isinstance(syllables_list[0], dict) and "S" in syllables_list[0]:
        syllables = json.dumps([syllable["S"].strip() for syllable in syllables_list])
    else:
        syllables = json.dumps([syllable.strip() for syllable in syllables_list])
    
    # Convert the syllable sounds list (of dicts) into a JSON string
    syllable_sounds = json.dumps(item.get("Syllable_Sounds", []))
    
    # Handle potential key naming differences for the image URL
    image_url = item.get("ImageUrl") or item.get("ImageURL", "")
    
    # Build the new item according to the target schema
    new_item = {
        "Identifier": item.get("Identifier", ""),
        "Level": item.get("Level", ""),
        "Base_Word": item.get("EnglishWord", ""),
        "Base_Lang_Code": "EN",  # Default base language code
        "Base_Lang_Options": english_options,
        "Targ_Word": item.get("SpanishWord", ""),
        "Targ_Lang_Code": "ES",  # Default target language code
        "Targ_Lang_Options": spanish_options,
        "Explanation_Word_Timing": item.get("Explanation_Word_Timing", ""),
        "Phonetic_Transcription": item.get("Phonetic_Transcription", ""),
        "Pronunciation_Explanation": item.get("Pronunciation_Explanation", ""),
        "Pronunciation_Explanation_Audio": item.get("Pronunciation_Explanation_Audio", ""),
        "Targ_Syllable": syllables,
        "Targ_Syllable_Sounds": syllable_sounds,
        "Word_Audio": item.get("Word_Audio", ""),
        "ImageURL": image_url
    }
    return new_item
def migrate_items():
    try:
        print("Starting migration...")
        response = source_table.scan()
        items = response.get('Items', [])
        
        # Check if items are in raw DynamoDB format (i.e. have type wrappers like 'S')
        if items and isinstance(next(iter(items[0].values())), dict) and 'S' in next(iter(items[0].values())):
            print("Deserializing items from raw DynamoDB format...")
            items = [deserialize_item(item) for item in items]
        
        # Handle pagination (if more than 1MB of data)
        while 'LastEvaluatedKey' in response:
            response = source_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            new_items = response.get('Items', [])
            if new_items and isinstance(next(iter(new_items[0].values())), dict) and 'S' in next(iter(new_items[0].values())):
                new_items = [deserialize_item(item) for item in new_items]
            items.extend(new_items)
        
        print(f"Found {len(items)} items in the source table.")
        
        # Use a batch writer to write items to the target table
        with target_table.batch_writer() as batch:
            for item in items:
                new_item = transform_item(item)
                batch.put_item(Item=new_item)
                print(f"Migrated item Identifier: {item.get('Identifier','')}, Level: {item.get('Level','')}")
                
        print("Migration completed successfully.")
    
    except ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")

if __name__ == '__main__':
    migrate_items()
