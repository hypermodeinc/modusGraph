import modusdb
import json
import os
import base64

print(f"Using data directory: {os.path.join(os.getcwd(), "data")}")

# Create the engine
engine = modusdb.Engine(os.path.join(os.getcwd(), "data"))

try:
    # Create namespace
    ns1 = engine.create_namespace()
        
    # Drop data and alter schema
    ns1.drop_data()
    ns1.alter_schema("name: string @index(exact) .")
        
    # Create mutation
    mutation = {
        "set": [{
            "subject": "_:aman",
            "predicate": "name",
            "object_value": {
                "str_val": "A"
            }
        }]
    }
        
    # Convert mutation to JSON string
    mutations_json = json.dumps([mutation])
        
    # Apply mutation
    result = ns1.mutate(mutations_json)
        
    # Query
    query = """{
        me(func: has(name)) {
            name
        }
    }"""
        
    response = ns1.query(query)
    response_dict = json.loads(response)
        
    # Decode the base64 JSON data
    json_data = base64.b64decode(response_dict['json']).decode('utf-8')
    actual_data = json.loads(json_data)
        
    expected = {"me": [{"name": "A"}]}
    assert actual_data == expected, f"Expected {expected}, got {actual_data}"
    print("Test passed successfully!")
        
finally:
    # Clean up
     engine.close()