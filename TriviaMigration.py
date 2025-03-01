import boto3
import json
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# AWS Configuration
REGION = 'us-east-1'
SOURCE_TABLE_NAME = 'dev-languageApp-spanishTriviaQuestions'  # Change this to your old table name
TARGET_TABLE_NAME = 'juno-middleware-languageApp-ChatterBoxTriviaQuestions'  # New table name

# Initialize AWS services
dynamodb = boto3.resource('dynamodb', region_name=REGION)
translate = boto3.client('translate', region_name=REGION)
source_table = dynamodb.Table(SOURCE_TABLE_NAME)
target_table = dynamodb.Table(TARGET_TABLE_NAME)

# Create a standard deserializer from boto3
deserializer = TypeDeserializer()
DYNAMO_TYPES = {"S", "N", "BOOL", "L", "M", "B", "SS", "NS", "BS"}

def custom_deserialize(value):
    """
    Recursively deserialize a value from DynamoDB JSON format.
    If the dict has one key and that key is a known DynamoDB type,
    attempt to deserialize it; otherwise, process recursively.
    """
    if isinstance(value, dict):
        if len(value) == 1 and list(value.keys())[0] in DYNAMO_TYPES:
            try:
                return deserializer.deserialize(value)
            except Exception as e:
                print("Error deserializing value:", value, e)
                return {k: custom_deserialize(v) for k, v in value.items()}
        else:
            return {k: custom_deserialize(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [custom_deserialize(item) for item in value]
    else:
        return value

def deserialize_item(raw_item):
    """Deserialize a raw DynamoDB item into a plain Python dict."""
    return {key: custom_deserialize(val) for key, val in raw_item.items()}

def translate_text(text, source_lang, target_lang):
    """Translate text using AWS Translate."""
    if not text:
        return ""
    try:
        response = translate.translate_text(
            Text=text,
            SourceLanguageCode=source_lang,
            TargetLanguageCode=target_lang
        )
        return response["TranslatedText"]
    except Exception as e:
        print("Translation error for text:", text, e)
        return text

def transform_item(item):
    """
    Transform the old trivia question item into the new schema.
    - The original question (assumed to be in Spanish) becomes Targ_Lang_Question.
    - We translate the question from Spanish to English for Base_Lang_Question.
    - Options (a list of strings in English) are stored as JSON in Base_Lang_Options.
      They are also translated to Spanish for Targ_Lang_Options.
    - The answer (in English) is stored as Base_Lang_Answer and translated to Spanish for Targ_Lang_Answer.
    """
    orig_question = item.get("question", "")
    orig_answer = item.get("answer", "")
    orig_options = item.get("options", [])  # Expecting a list of strings
    
    # Translate question from Spanish to English
    base_question = translate_text(orig_question, "es", "en")
    # Translate each option from English to Spanish
    targ_options = [translate_text(opt, "en", "es") for opt in orig_options]
    # Translate answer from English to Spanish
    targ_answer = translate_text(orig_answer, "en", "es")
    
    new_item = {
        "identifier": item.get("identifier", ""),
        "level": item.get("level", ""),
        "Base_Lang_Code": "EN",
        "Base_Lang_Question": base_question,
        "Base_Lang_Options": json.dumps(orig_options, ensure_ascii=False),
        "Base_Lang_Answer": orig_answer,
        "Targ_Lang_Code": "ES",
        "Targ_Lang_Question": orig_question,
        "Targ_Lang_Options": json.dumps(targ_options, ensure_ascii=False),
        "Targ_Lang_Answer": targ_answer,
        "imageURL": item.get("imageUrl", "")
    }
    return new_item

def migrate_items():
    try:
        print("Starting trivia questions migration...")
        all_items = []
        response = source_table.scan()
        all_items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = source_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            all_items.extend(response.get("Items", []))
        print(f"Found {len(all_items)} items in the source table.\n")
        
        # Deserialize all items
        deserialized_items = [deserialize_item(raw) for raw in all_items]
        
        # Optional: Print an example item for debugging
        if deserialized_items:
            print("-- Example deserialized item --")
            print(json.dumps(deserialized_items[0], indent=2, ensure_ascii=False))
        
        # Transform and write each item to the target table
        with target_table.batch_writer() as batch:
            for idx, item in enumerate(deserialized_items):
                new_item = transform_item(item)
                batch.put_item(Item=new_item)
                print(f"Migrated {idx+1}/{len(deserialized_items)}: {new_item['identifier']}")
        
        print("\nMigration completed successfully.")
    
    except ClientError as e:
        print("An error occurred:", e.response["Error"]["Message"])

if __name__ == "__main__":
    migrate_items()
