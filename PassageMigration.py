import boto3
import json
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# AWS Configuration
REGION = 'us-east-1'
SOURCE_TABLE_NAME = 'dev-languageApp-spanishPassages'           # Adjust to actual source table name
TARGET_TABLE_NAME = 'juno-middleware-languageApp-ChatterBoxPassages'  # Target table name

# Initialize AWS services
dynamodb = boto3.resource('dynamodb', region_name=REGION)
translate = boto3.client('translate', region_name=REGION)
source_table = dynamodb.Table(SOURCE_TABLE_NAME)
target_table = dynamodb.Table(TARGET_TABLE_NAME)

# Deserializer for DynamoDB JSON format
deserializer = TypeDeserializer()

def deserialize_item(item):
    """Convert a DynamoDB item with type wrappers into a plain Python dict."""
    deserialized = {}
    
    for k, v in item.items():
        if isinstance(v, dict) and len(v) == 1 and isinstance(next(iter(v.values())), (str, list, dict)):
            # Deserialize if it's still in DynamoDB JSON format
            deserialized[k] = deserializer.deserialize(v)
        else:
            # It's already deserialized
            deserialized[k] = v

    return deserialized

def translate_text(text, source_lang="es", target_lang="en"):
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

def process_options(options_list):
    """Convert a list of options to a JSON string."""
    if not options_list:
        return "[]"
    return json.dumps([opt for opt in options_list])

def transform_item(item):
    """Transform a deserialized source item into the target schema."""
    
    # Translate passage metadata to English
    base_lang_title = translate_text(item.get("#name", ""))
    base_lang_description = translate_text(item.get("Description", ""))
    base_lang_passage = translate_text(item.get("Passage", ""))
    base_lang_answers = [
        translate_text(item.get("Answer_1", "")),
        translate_text(item.get("Answer_2", "")),
        translate_text(item.get("Answer_3", "")),
        translate_text(item.get("Answer_4", ""))
    ]
    base_lang_questions = [
        translate_text(item.get("Question_1", "")),
        translate_text(item.get("Question_2", "")),
        translate_text(item.get("Question_3", "")),
        translate_text(item.get("Question_4", ""))
    ]
    
    # Extract multiple-choice options and convert to JSON
    base_lang_options = [
        process_options(item.get("Options_1", [])),
        process_options(item.get("Options_2", [])),
        process_options(item.get("Options_3", [])),
        process_options(item.get("Options_4", []))
    ]
    
    # Extract passage word timings and convert to JSON string
    passage_word_timings = json.dumps(item.get("Passage_Word_Timings", "[]"))

    new_item = {
        "Identifier": item.get("Identifier", ""),
        "Level": item.get("Level", ""),
        "Genre": item.get("Genre", ""),
        
        "Base_Lang_Code": "EN",
        "Base_Lang_Title": base_lang_title,
        "Base_Lang_Description": base_lang_description,
        "Base_Lang_Answer_1": base_lang_answers[0],
        "Base_Lang_Answer_2": base_lang_answers[1],
        "Base_Lang_Answer_3": base_lang_answers[2],
        "Base_Lang_Answer_4": base_lang_answers[3],
        "Base_Lang_Options_1": base_lang_options[0],
        "Base_Lang_Options_2": base_lang_options[1],
        "Base_Lang_Options_3": base_lang_options[2],
        "Base_Lang_Options_4": base_lang_options[3],
        "Base_Lang_Passage": base_lang_passage,
        "Base_Lang_Question_1": base_lang_questions[0],
        "Base_Lang_Question_2": base_lang_questions[1],
        "Base_Lang_Question_3": base_lang_questions[2],
        "Base_Lang_Question_4": base_lang_questions[3],

        "Targ_Lang_Code": "ES",
        "Targ_Lang_Title": item.get("#name", ""),
        "Targ_Lang_Description": item.get("Description", ""),
        "Targ_Lang_Answer_1": item.get("Answer_1", ""),
        "Targ_Lang_Answer_2": item.get("Answer_2", ""),
        "Targ_Lang_Answer_3": item.get("Answer_3", ""),
        "Targ_Lang_Answer_4": item.get("Answer_4", ""),
        "Targ_Lang_Options_1": process_options(item.get("Options_1", [])),
        "Targ_Lang_Options_2": process_options(item.get("Options_2", [])),
        "Targ_Lang_Options_3": process_options(item.get("Options_3", [])),
        "Targ_Lang_Options_4": process_options(item.get("Options_4", [])),
        "Targ_Lang_Passage": item.get("Passage", ""),
        "Targ_Passage_Word_Timings": passage_word_timings,
        "Targ_Passage_Audio_URL": item.get("Passage_Audio_URL", ""),
        "Targ_Lang_Question_1": item.get("Question_1", ""),
        "Targ_Lang_Question_2": item.get("Question_2", ""),
        "Targ_Lang_Question_3": item.get("Question_3", ""),
        "Targ_Lang_Question_4": item.get("Question_4", ""),
        
        "ImageURL": item.get("ImageUrl", ""),
        "Prompt": item.get("Prompt", ""),
        "Section": item.get("Level", "")
    }
    return new_item

def migrate_items():
    try:
        print("Starting passage migration...")
        response = source_table.scan()
        items = response.get('Items', [])
        
        items = [deserialize_item(item) for item in items]
        
        with target_table.batch_writer() as batch:
            for item in items:
                new_item = transform_item(item)
                batch.put_item(Item=new_item)
                print(f"Migrated passage: {new_item.get('Identifier')}, Title: {new_item.get('Targ_Lang_Title')}")
                
        print("Migration completed successfully.")
    
    except ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")

if __name__ == '__main__':
    migrate_items()
