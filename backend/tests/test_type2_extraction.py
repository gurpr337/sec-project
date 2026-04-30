import sys
import os

# Dynamic path to backend
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from bs4 import BeautifulSoup
from app.services.extractors.type2_extractor import Type2_Extractor

def parse_numeric_value(text):
    """Parse numeric value, handling parentheses as negative"""
    if not text or text.strip() in ['—', '-', '']:
        return 0.0

    text = text.strip()

    # Handle parentheses as negative
    is_negative = False
    if text.startswith('(') and text.endswith(')'):
        is_negative = True
        text = text[1:-1]

    # Remove $ and commas
    text = text.replace('$', '').replace(',', '')

    try:
        value = float(text)
        return -value if is_negative else value
    except ValueError:
        return 0.0

def get_expected_output():
    """Return the expected output structure as described by user"""
    return {
        "headers": ["Metric", "Three Months Ended June 30, 2025", "Three Months Ended June 30, 2024"],
        "data": [
            # Premiums section
            {"metric": "Premiums", "section": "Revenues - unaffiliated customers", "segment": "UnitedHealthcare", "2025": 83019, "2024": 70950},
            {"metric": "Premiums", "section": "Revenues - unaffiliated customers", "segment": "Optum Health", "2025": 4886, "2024": 5947},
            {"metric": "Premiums", "section": "Revenues - unaffiliated customers", "segment": "Optum Insight", "2025": 0, "2024": 0},
            {"metric": "Premiums", "section": "Revenues - unaffiliated customers", "segment": "Optum Rx", "2025": 0, "2024": 0},
            {"metric": "Premiums", "section": "Revenues - unaffiliated customers", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Premiums", "section": "Revenues - unaffiliated customers", "segment": "Optum", "2025": 4886, "2024": 5947},
            {"metric": "Premiums", "section": "Revenues - unaffiliated customers", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Premiums", "section": "Revenues - unaffiliated customers", "segment": "Consolidated", "2025": 87905, "2024": 76897},

            # Products section
            {"metric": "Products", "section": "Revenues - unaffiliated customers", "segment": "UnitedHealthcare", "2025": 0, "2024": 0},
            {"metric": "Products", "section": "Revenues - unaffiliated customers", "segment": "Optum Health", "2025": 65, "2024": 62},
            {"metric": "Products", "section": "Revenues - unaffiliated customers", "segment": "Optum Insight", "2025": 44, "2024": 41},
            {"metric": "Products", "section": "Revenues - unaffiliated customers", "segment": "Optum Rx", "2025": 13455, "2024": 12108},
            {"metric": "Products", "section": "Revenues - unaffiliated customers", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Products", "section": "Revenues - unaffiliated customers", "segment": "Optum", "2025": 13564, "2024": 12211},
            {"metric": "Products", "section": "Revenues - unaffiliated customers", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Products", "section": "Revenues - unaffiliated customers", "segment": "Consolidated", "2025": 13564, "2024": 12211},

            # Services section
            {"metric": "Services", "section": "Revenues - unaffiliated customers", "segment": "UnitedHealthcare", "2025": 2511, "2024": 2388},
            {"metric": "Services", "section": "Revenues - unaffiliated customers", "segment": "Optum Health", "2025": 3846, "2024": 4083},
            {"metric": "Services", "section": "Revenues - unaffiliated customers", "segment": "Optum Insight", "2025": 1516, "2024": 1405},
            {"metric": "Services", "section": "Revenues - unaffiliated customers", "segment": "Optum Rx", "2025": 1166, "2024": 874},
            {"metric": "Services", "section": "Revenues - unaffiliated customers", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Services", "section": "Revenues - unaffiliated customers", "segment": "Optum", "2025": 6528, "2024": 6362},
            {"metric": "Services", "section": "Revenues - unaffiliated customers", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Services", "section": "Revenues - unaffiliated customers", "segment": "Consolidated", "2025": 9039, "2024": 8750},

            # Total revenues - unaffiliated customers
            {"metric": "Total revenues - unaffiliated customers", "section": "Revenues - unaffiliated customers", "segment": "UnitedHealthcare", "2025": 85530, "2024": 73338},
            {"metric": "Total revenues - unaffiliated customers", "section": "Revenues - unaffiliated customers", "segment": "Optum Health", "2025": 8797, "2024": 10092},
            {"metric": "Total revenues - unaffiliated customers", "section": "Revenues - unaffiliated customers", "segment": "Optum Insight", "2025": 1560, "2024": 1446},
            {"metric": "Total revenues - unaffiliated customers", "section": "Revenues - unaffiliated customers", "segment": "Optum Rx", "2025": 14621, "2024": 12982},
            {"metric": "Total revenues - unaffiliated customers", "section": "Revenues - unaffiliated customers", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Total revenues - unaffiliated customers", "section": "Revenues - unaffiliated customers", "segment": "Optum", "2025": 24978, "2024": 24520},
            {"metric": "Total revenues - unaffiliated customers", "section": "Revenues - unaffiliated customers", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Total revenues - unaffiliated customers", "section": "Revenues - unaffiliated customers", "segment": "Consolidated", "2025": 110508, "2024": 97858},

            # Total revenues - affiliated customers
            {"metric": "Total revenues - affiliated customers", "section": "Total revenues - affiliated customers", "segment": "UnitedHealthcare", "2025": 0, "2024": 0},
            {"metric": "Total revenues - affiliated customers", "section": "Total revenues - affiliated customers", "segment": "Optum Health", "2025": 15953, "2024": 16576},
            {"metric": "Total revenues - affiliated customers", "section": "Total revenues - affiliated customers", "segment": "Optum Insight", "2025": 3236, "2024": 3070},
            {"metric": "Total revenues - affiliated customers", "section": "Total revenues - affiliated customers", "segment": "Optum Rx", "2025": 23790, "2024": 19373},
            {"metric": "Total revenues - affiliated customers", "section": "Total revenues - affiliated customers", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Total revenues - affiliated customers", "section": "Total revenues - affiliated customers", "segment": "Optum", "2025": 41712, "2024": 37890},
            {"metric": "Total revenues - affiliated customers", "section": "Total revenues - affiliated customers", "segment": "Corporate and Eliminations", "2025": -41712, "2024": -37890},
            {"metric": "Total revenues - affiliated customers", "section": "Total revenues - affiliated customers", "segment": "Consolidated", "2025": 0, "2024": 0},

            # Investment and other income
            {"metric": "Investment and other income", "section": "Investment and other income", "segment": "UnitedHealthcare", "2025": 573, "2024": 528},
            {"metric": "Investment and other income", "section": "Investment and other income", "segment": "Optum Health", "2025": 455, "2024": 382},
            {"metric": "Investment and other income", "section": "Investment and other income", "segment": "Optum Insight", "2025": 32, "2024": 27},
            {"metric": "Investment and other income", "section": "Investment and other income", "segment": "Optum Rx", "2025": 48, "2024": 60},
            {"metric": "Investment and other income", "section": "Investment and other income", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Investment and other income", "section": "Investment and other income", "segment": "Optum", "2025": 535, "2024": 469},
            {"metric": "Investment and other income", "section": "Investment and other income", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Investment and other income", "section": "Investment and other income", "segment": "Consolidated", "2025": 1108, "2024": 997},

            # Total revenues
            {"metric": "Total revenues", "section": "Total revenues", "segment": "UnitedHealthcare", "2025": 86103, "2024": 73866},
            {"metric": "Total revenues", "section": "Total revenues", "segment": "Optum Health", "2025": 25205, "2024": 27050},
            {"metric": "Total revenues", "section": "Total revenues", "segment": "Optum Insight", "2025": 4828, "2024": 4543},
            {"metric": "Total revenues", "section": "Total revenues", "segment": "Optum Rx", "2025": 38459, "2024": 32415},
            {"metric": "Total revenues", "section": "Total revenues", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Total revenues", "section": "Total revenues", "segment": "Optum", "2025": 67225, "2024": 62879},
            {"metric": "Total revenues", "section": "Total revenues", "segment": "Corporate and Eliminations", "2025": -41712, "2024": -37890},
            {"metric": "Total revenues", "section": "Total revenues", "segment": "Consolidated", "2025": 111616, "2024": 98855},

            # Total operating costs (a)
            {"metric": "Total operating costs (a)", "section": "Total operating costs (a)", "segment": "UnitedHealthcare", "2025": 84028, "2024": 69862},
            {"metric": "Total operating costs (a)", "section": "Total operating costs (a)", "segment": "Optum Health", "2025": 24569, "2024": 25131},
            {"metric": "Total operating costs (a)", "section": "Total operating costs (a)", "segment": "Optum Insight", "2025": 3830, "2024": 3997},
            {"metric": "Total operating costs (a)", "section": "Total operating costs (a)", "segment": "Optum Rx", "2025": 37018, "2024": 31009},
            {"metric": "Total operating costs (a)", "section": "Total operating costs (a)", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Total operating costs (a)", "section": "Total operating costs (a)", "segment": "Optum", "2025": 64150, "2024": 59008},
            {"metric": "Total operating costs (a)", "section": "Total operating costs (a)", "segment": "Corporate and Eliminations", "2025": -1267, "2024": -1129},
            {"metric": "Total operating costs (a)", "section": "Total operating costs (a)", "segment": "Consolidated", "2025": 106466, "2024": 90980},

            # Earnings from operations
            {"metric": "Earnings from operations", "section": "Earnings from operations", "segment": "UnitedHealthcare", "2025": 2075, "2024": 4004},
            {"metric": "Earnings from operations", "section": "Earnings from operations", "segment": "Optum Health", "2025": 636, "2024": 1919},
            {"metric": "Earnings from operations", "section": "Earnings from operations", "segment": "Optum Insight", "2025": 998, "2024": 546},
            {"metric": "Earnings from operations", "section": "Earnings from operations", "segment": "Optum Rx", "2025": 1441, "2024": 1406},
            {"metric": "Earnings from operations", "section": "Earnings from operations", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Earnings from operations", "section": "Earnings from operations", "segment": "Optum", "2025": 3075, "2024": 3871},
            {"metric": "Earnings from operations", "section": "Earnings from operations", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Earnings from operations", "section": "Earnings from operations", "segment": "Consolidated", "2025": 5150, "2024": 7875},

            # Interest expense
            {"metric": "Interest expense", "section": "Interest expense", "segment": "UnitedHealthcare", "2025": 0, "2024": 0},
            {"metric": "Interest expense", "section": "Interest expense", "segment": "Optum Health", "2025": 0, "2024": 0},
            {"metric": "Interest expense", "section": "Interest expense", "segment": "Optum Insight", "2025": 0, "2024": 0},
            {"metric": "Interest expense", "section": "Interest expense", "segment": "Optum Rx", "2025": 0, "2024": 0},
            {"metric": "Interest expense", "section": "Interest expense", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Interest expense", "section": "Interest expense", "segment": "Optum", "2025": 0, "2024": 0},
            {"metric": "Interest expense", "section": "Interest expense", "segment": "Corporate and Eliminations", "2025": 0, "2024": -985},
            {"metric": "Interest expense", "section": "Interest expense", "segment": "Consolidated", "2025": -1027, "2024": -985},

            # Loss on sale of subsidiary and subsidiaries held for sale
            {"metric": "Loss on sale of subsidiary and subsidiaries held for sale", "section": "Loss on sale of subsidiary and subsidiaries held for sale", "segment": "UnitedHealthcare", "2025": -41, "2024": -1225},
            {"metric": "Loss on sale of subsidiary and subsidiaries held for sale", "section": "Loss on sale of subsidiary and subsidiaries held for sale", "segment": "Optum Health", "2025": 0, "2024": 0},
            {"metric": "Loss on sale of subsidiary and subsidiaries held for sale", "section": "Loss on sale of subsidiary and subsidiaries held for sale", "segment": "Optum Insight", "2025": 0, "2024": 0},
            {"metric": "Loss on sale of subsidiary and subsidiaries held for sale", "section": "Loss on sale of subsidiary and subsidiaries held for sale", "segment": "Optum Rx", "2025": 0, "2024": 0},
            {"metric": "Loss on sale of subsidiary and subsidiaries held for sale", "section": "Loss on sale of subsidiary and subsidiaries held for sale", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Loss on sale of subsidiary and subsidiaries held for sale", "section": "Loss on sale of subsidiary and subsidiaries held for sale", "segment": "Optum", "2025": 0, "2024": 0},
            {"metric": "Loss on sale of subsidiary and subsidiaries held for sale", "section": "Loss on sale of subsidiary and subsidiaries held for sale", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Loss on sale of subsidiary and subsidiaries held for sale", "section": "Loss on sale of subsidiary and subsidiaries held for sale", "segment": "Consolidated", "2025": -41, "2024": -1225},

            # Earnings before income taxes
            {"metric": "Earnings before income taxes", "section": "Earnings before income taxes", "segment": "UnitedHealthcare", "2025": 2034, "2024": 2779},
            {"metric": "Earnings before income taxes", "section": "Earnings before income taxes", "segment": "Optum Health", "2025": 636, "2024": 1919},
            {"metric": "Earnings before income taxes", "section": "Earnings before income taxes", "segment": "Optum Insight", "2025": 998, "2024": 546},
            {"metric": "Earnings before income taxes", "section": "Earnings before income taxes", "segment": "Optum Rx", "2025": 1441, "2024": 1406},
            {"metric": "Earnings before income taxes", "section": "Earnings before income taxes", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Earnings before income taxes", "section": "Earnings before income taxes", "segment": "Optum", "2025": 3075, "2024": 3871},
            {"metric": "Earnings before income taxes", "section": "Earnings before income taxes", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Earnings before income taxes", "section": "Earnings before income taxes", "segment": "Consolidated", "2025": 4082, "2024": 5665},

            # Total assets
            {"metric": "Total assets", "section": "Total assets", "segment": "UnitedHealthcare", "2025": 129587, "2024": 109441},
            {"metric": "Total assets", "section": "Total assets", "segment": "Optum Health", "2025": 96452, "2024": 93858},
            {"metric": "Total assets", "section": "Total assets", "segment": "Optum Insight", "2025": 33716, "2024": 34244},
            {"metric": "Total assets", "section": "Total assets", "segment": "Optum Rx", "2025": 61674, "2024": 56058},
            {"metric": "Total assets", "section": "Total assets", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Total assets", "section": "Total assets", "segment": "Optum", "2025": 191842, "2024": 184160},
            {"metric": "Total assets", "section": "Total assets", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Total assets", "section": "Total assets", "segment": "Consolidated", "2025": 308573, "2024": 286056},

            # Purchases of property, equipment and capitalized software
            {"metric": "Purchases of property, equipment and capitalized software", "section": "Purchases of property, equipment and capitalized software", "segment": "UnitedHealthcare", "2025": 193, "2024": 187},
            {"metric": "Purchases of property, equipment and capitalized software", "section": "Purchases of property, equipment and capitalized software", "segment": "Optum Health", "2025": 306, "2024": 230},
            {"metric": "Purchases of property, equipment and capitalized software", "section": "Purchases of property, equipment and capitalized software", "segment": "Optum Insight", "2025": 289, "2024": 344},
            {"metric": "Purchases of property, equipment and capitalized software", "section": "Purchases of property, equipment and capitalized software", "segment": "Optum Rx", "2025": 98, "2024": 92},
            {"metric": "Purchases of property, equipment and capitalized software", "section": "Purchases of property, equipment and capitalized software", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Purchases of property, equipment and capitalized software", "section": "Purchases of property, equipment and capitalized software", "segment": "Optum", "2025": 693, "2024": 666},
            {"metric": "Purchases of property, equipment and capitalized software", "section": "Purchases of property, equipment and capitalized software", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Purchases of property, equipment and capitalized software", "section": "Purchases of property, equipment and capitalized software", "segment": "Consolidated", "2025": 886, "2024": 853},

            # Depreciation and Amortization
            {"metric": "Depreciation and Amortization", "section": "Depreciation and Amortization", "segment": "UnitedHealthcare", "2025": 221, "2024": 221},
            {"metric": "Depreciation and Amortization", "section": "Depreciation and Amortization", "segment": "Optum Health", "2025": 296, "2024": 277},
            {"metric": "Depreciation and Amortization", "section": "Depreciation and Amortization", "segment": "Optum Insight", "2025": 351, "2024": 316},
            {"metric": "Depreciation and Amortization", "section": "Depreciation and Amortization", "segment": "Optum Rx", "2025": 216, "2024": 206},
            {"metric": "Depreciation and Amortization", "section": "Depreciation and Amortization", "segment": "Optum Eliminations", "2025": 0, "2024": 0},
            {"metric": "Depreciation and Amortization", "section": "Depreciation and Amortization", "segment": "Optum", "2025": 863, "2024": 799},
            {"metric": "Depreciation and Amortization", "section": "Depreciation and Amortization", "segment": "Corporate and Eliminations", "2025": 0, "2024": 0},
            {"metric": "Depreciation and Amortization", "section": "Depreciation and Amortization", "segment": "Consolidated", "2025": 1084, "2024": 1020},
        ]
    }

def extract_actual_output(html_content):
    """Extract the actual output from our Type 2 extractor"""
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')

    extractor = Type2_Extractor()

    # Extract headers and data
    headers = extractor._extract_table_headers_simple(table)
    extracted_data = extractor._extract_table_data_simple(table)

    print(f"\nDEBUG: Extracted headers: {headers}")
    print(f"DEBUG: Number of extracted rows: {len(extracted_data)}")

    # Show a sample row
    for i, row in enumerate(extracted_data):
        if i >= 5:  # Show first few rows
            break
        if row and len(row) > 1:
            cells = [cell.get('text', '') for cell in row]
            print(f"DEBUG Row {i}: {cells}")

    # Transform the data
    transformation = extractor._transform_type2_table_data(extracted_data, headers)
    transformed_data = transformation['transformed_data']
    new_headers = transformation['new_headers']

    return new_headers, transformed_data

def test_type2_extraction():
    """Test Type 2 extraction against expected results"""

    # Dynamic path to test file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_file_path = os.path.join(current_dir, 'app', 'test_files', 'type2table.html')
    with open(test_file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Get expected output
    expected = get_expected_output()

    # Get actual output
    actual_headers, actual_data = extract_actual_output(html_content)

    print("=== TESTING TYPE 2 EXTRACTION ===")
    print(f"Expected headers: {expected['headers']}")
    print(f"Actual headers: {actual_headers}")
    print()

    # Check headers match
    if actual_headers != expected['headers']:
        print("❌ Headers don't match!")
        print(f"Expected: {expected['headers']}")
        print(f"Actual: {actual_headers}")
        return False

    print("✅ Headers match!")

    # Check if we have the right number of rows (should be 8 segments × number of metrics)
    expected_unique_metrics = set()
    for item in expected['data']:
        key = (item['metric'], item['segment'])
        expected_unique_metrics.add(key)

    print(f"Expected unique metric-segment combinations: {len(expected_unique_metrics)}")
    print(f"Actual rows: {len(actual_data)}")

    # Check first few actual rows to see the metric format
    if actual_data:
        print("Sample actual metrics:")
        for i in range(min(5, len(actual_data))):
            if actual_data[i] and len(actual_data[i]) > 0:
                print(f"  {actual_data[i][0]['text']}")

    # Check data
    expected_data = expected['data']
    print(f"Expected {len(expected_data)} data points")
    print(f"Actual {len(actual_data)} rows")

    # Convert actual data to comparable format
    actual_data_flat = []
    for row in actual_data:
        if len(row) < 2:
            continue

        metric_cell = row[0]
        metric_name = metric_cell.get('text', '').strip()

        # Skip section headers
        if metric_cell.get('is_section_header', False):
            continue

        for i, cell in enumerate(row[1:], 0):
            if i >= len(actual_headers) - 1:  # -1 because first header is "Metric"
                break

            date_header = actual_headers[i + 1]  # Skip "Metric" header
            value_text = cell.get('text', '').strip()
            value = parse_numeric_value(value_text)

            actual_data_flat.append({
                'metric': metric_name,
                'date': date_header,
                'value': value
            })

    # Compare data points
    matches = 0
    mismatches = 0

    for expected_point in expected_data:
        metric = expected_point['metric']
        segment = expected_point['segment']
        section = expected_point['section']

        # Create flattened metric name like our system does
        flattened_metric = f"{section} :: {metric} :: {segment}"

        # Check both dates
        for year_suffix, expected_value in [('2025', expected_point['2025']), ('2024', expected_point['2024'])]:
            date_header = f"Three Months Ended June 30, {year_suffix}"

            # Find matching actual point
            actual_point = None
            for point in actual_data_flat:
                if point['metric'] == flattened_metric and point['date'] == date_header:
                    actual_point = point
                    break

            if actual_point:
                if abs(actual_point['value'] - expected_value) < 0.01:  # Allow small floating point differences
                    matches += 1
                else:
                    mismatches += 1
                    if mismatches < 5:  # Only show first few mismatches
                        print(f"❌ Value mismatch for {flattened_metric} on {date_header}:")
                        print(f"   Expected: {expected_value}, Actual: {actual_point['value']}")
            else:
                mismatches += 1
                if mismatches < 5:  # Only show first few missing points
                    print(f"❌ Missing data point for {flattened_metric} on {date_header}")

    print(f"\nResults: {matches} matches, {mismatches} mismatches")

    if mismatches == 0:
        print("🎉 All tests passed!")
        return True
    else:
        print("💥 Tests failed!")
        return False

if __name__ == "__main__":
    success = test_type2_extraction()
    if not success:
        sys.exit(1)
