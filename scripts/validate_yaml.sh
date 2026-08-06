#!/bin/bash
# =============================================================================
# Validate YAML Script — Banking Data Platform
# =============================================================================
# Usage: ./scripts/validate_yaml.sh
# =============================================================================

set -e

echo "=========================================="
echo " Banking Data Platform — YAML Validation"
echo "=========================================="
echo ""

python3 -c "
import yaml
from pathlib import Path
import sys

errors = []

# Validate ETL YAML configs
print('=== Validating Bronze YAML configs ===')
for yaml_file in Path('code_etl/bronze').rglob('*.yml'):
    try:
        with open(yaml_file) as f:
            yaml.safe_load(f)
        print(f'  ✅ {yaml_file.name}')
    except Exception as e:
        errors.append(f'{yaml_file}: {e}')
        print(f'  ❌ {yaml_file.name}: {e}')

print()
print('=== Validating Silver YAML configs ===')
for yaml_file in Path('code_etl/silver').rglob('*.yml'):
    try:
        with open(yaml_file) as f:
            yaml.safe_load(f)
        print(f'  ✅ {yaml_file.name}')
    except Exception as e:
        errors.append(f'{yaml_file}: {e}')
        print(f'  ❌ {yaml_file.name}: {e}')

print()
print('=== Validating Gold YAML configs ===')
for yaml_file in Path('code_etl/gold').rglob('*.yml'):
    try:
        with open(yaml_file) as f:
            yaml.safe_load(f)
        print(f'  ✅ {yaml_file.name}')
    except Exception as e:
        errors.append(f'{yaml_file}: {e}')
        print(f'  ❌ {yaml_file.name}: {e}')

print()
print('=== Validating CDC YAML configs ===')
for yaml_file in Path('code_etl/cdc/config').rglob('*.yml'):
    try:
        with open(yaml_file) as f:
            yaml.safe_load(f)
        print(f'  ✅ {yaml_file.name}')
    except Exception as e:
        errors.append(f'{yaml_file}: {e}')
        print(f'  ❌ {yaml_file.name}: {e}')

print()
print('=== Validating Data Contract YAMLs ===')
for yaml_file in Path('governance/datasets').glob('*.yaml'):
    try:
        with open(yaml_file) as f:
            data = yaml.safe_load(f)
        # Check required fields
        required = ['dataset_id', 'owner', 'business_purpose', 'layer', 'physical_location']
        for field in required:
            if field not in data:
                errors.append(f'{yaml_file.name}: missing field \"{field}\"')
        print(f'  ✅ {yaml_file.name}')
    except Exception as e:
        errors.append(f'{yaml_file}: {e}')
        print(f'  ❌ {yaml_file.name}: {e}')

print()
print('=== Validating DQ Rules YAML ===')
try:
    with open('code_etl/shared/ops/dq_rules.yml') as f:
        yaml.safe_load(f)
    print('  ✅ dq_rules.yml')
except Exception as e:
    errors.append(f'dq_rules.yml: {e}')
    print(f'  ❌ dq_rules.yml: {e}')

print()
print('==========================================')
if errors:
    print(f'❌ {len(errors)} YAML validation errors found')
    for err in errors:
        print(f'  - {err}')
    sys.exit(1)
else:
    print('✅ All YAML configs are valid')
    sys.exit(0)
"
