import sys
import os

# Dynamic path to backend
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

import requests
from bs4 import BeautifulSoup
from app.services.extractors.type2_extractor import Type2_Extractor

def test_optum_table_extraction():
    """Test that the Optum table is correctly processed as Type 2"""

    print("Testing Optum table Type 2 extraction...")

    # Fetch the document
    url = 'https://www.sec.gov/Archives/edgar/data/731766/000073176625000236/unh-20250630.htm'
    response = requests.get(url, headers={'User-Agent': 'SECExtractor/1.0 (contact@example.com)'})
    soup = BeautifulSoup(response.content, 'html.parser')

    # Get table 23 (Optum table)
    tables = soup.find_all('table')
    optum_table = tables[23]

    # Test extraction
    extractor = Type2_Extractor()

    # Extract headers and data
    headers = extractor._extract_table_headers_simple(optum_table)
    extracted_data = extractor._extract_table_data_simple(optum_table)

    print(f"Extracted {len(headers)} headers")
    print(f"Extracted {len(extracted_data)} rows of data")

    # Check if it's detected as segment-based
    is_segment_based = extractor._is_segment_based_table(extracted_data)
    print(f"Is segment-based table: {is_segment_based}")

    # Debug: show some extracted data rows
    print("\\nSample extracted data rows:")
    for i, row in enumerate(extracted_data[:10]):
        if row:
            row_texts = [cell.get('text', '')[:15] for cell in row]
            is_header = row[0].get('is_section_header', False) if row else False
            print(f"  Row {i}: {len(row)} cells - {row_texts[:5]}... (header: {is_header})")

    # Transform data
    transformation = extractor._transform_type2_table_data(extracted_data, headers)
    transformed_data = transformation['transformed_data']
    new_headers = transformation['new_headers']

    print(f"\\nTransformation results:")
    print(f"New headers: {new_headers}")
    print(f"Transformed rows: {len(transformed_data)}")

    # Check specific expected results
    expected_headers = ['Metric', 'June 30, 2025', 'June 30, 2024']
    if new_headers == expected_headers:
        print("✅ Headers are correct")
    else:
        print(f"❌ Headers mismatch. Expected: {expected_headers}, Got: {new_headers}")

    # Check for expected metrics and data
    premiums_found = False
    products_found = False
    services_found = False

    for row in transformed_data:
        metric = row[0]['text'].strip()
        values = [cell['text'].strip() for cell in row[1:]]

        if metric == 'Premiums':
            premiums_found = True
            print(f"✅ Premiums: {values}")
            # Should have values for both dates
            if len(values) == 2 and all(v for v in values):
                print("   ✅ Both dates have values")
            else:
                print("   ❌ Missing values")

        elif metric == 'Products':
            products_found = True
            print(f"✅ Products: {values}")

        elif metric == 'Services':
            services_found = True
            print(f"✅ Services: {values}")

    if premiums_found and products_found and services_found:
        print("\\n✅ All expected metrics found")
        return True
    else:
        print("\\n❌ Missing expected metrics")
        return False

if __name__ == "__main__":
    success = test_optum_table_extraction()
    if success:
        print("\\n🎉 Optum table extraction test PASSED!")
    else:
        print("\\n💥 Optum table extraction test FAILED!")
        sys.exit(1)
