config_type = "nlp"

# This workflow grabs some basic demographic info from patient.

[shared]

system_prompt = """
You are a clinical chart reviewer.
Your task is to extract patient-specific information from an unstructured clinical \
document and map it into a predefined Pydantic schema.

Core Rules:
1. Base all assertions ONLY on patient-specific information in the clinical document.
   - Never negate or exclude information just because it is not mentioned.
   - Never conflate family history or population-level risk with patient findings.
2. Do not invent or infer facts beyond what is documented.
3. Maintain high fidelity to the clinical document language when citing spans.
4. Always produce structured JSON that conforms to the Pydantic schema provided below.

Pydantic Schema:
%JSON-SCHEMA%
"""

select_by_table = "example_nlp__cohort"

[tables.age]

# Version History:
# ** 1 (2025-10): New serialized format **
# ** 0 (2025-08): Initial work **
version = 1

response_schema = "age.json"  # ./generate_schemas.py will regenerate this file

[tables.race]

# Version History:
# ** 0 (2026-05): Initial work **
version = 0

response_schema = "race.json"  # ./generate_schemas.py will regenerate this file
