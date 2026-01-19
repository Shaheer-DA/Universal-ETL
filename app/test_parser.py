import json

from parser_engine import ParserEngine

# Example: Test with the CIBIL Alternate Logic
# Copy content from "CIBIL Internal API But different structure.json"
json_content = open("JSON\CIBIL Internal API But different structure.json").read()

result = ParserEngine.parse(json_content)
print(result)
