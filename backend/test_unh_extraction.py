import sys
import os

# Dynamic path to backend
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from app.services.sec_extractor import SECExtractor

def test_unh_extraction():
    # Load the UNH HTML
    with open('unh_full.html', 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Initialize extractor
    extractor = SECExtractor()

    # Parse tables (use dummy filing_id)
    tables = extractor.parse_html_tables(html_content, 'test_filing_id')

    print(f"Found {len(tables)} tables")

    # Find the "Loss on sale of subsidiary and subsidiaries held for sale" table
    target_table = None
    for table_data in tables:
        title = table_data.get('title', '').lower()
        if 'loss on sale of subsidiary' in title or 'subsidiaries held for sale' in title:
            target_table = table_data
            break

    if not target_table:
        print("Target table not found!")
        # Print all table titles for debugging
        for i, table_data in enumerate(tables):
            print(f"Table {i}: {table_data.get('title', 'No title')}")
        return

    print(f"Found target table: {target_table['title']}")
    print(f"Headers: {target_table['headers']}")
    print(f"Number of rows: {len(target_table['extracted_data'])}")
    print(f"Is Type 2 transformed: {target_table.get('is_type2_transformed', False)}")

    # Check if it's a Type 2 table
    is_type2 = extractor.is_type2_table(target_table['extracted_data'])
    print(f"Is Type 2 table: {is_type2}")

    if is_type2:
        print("This is a Type 2 table, let's examine the raw data:")
        for i, row in enumerate(target_table['extracted_data'][:10]):  # First 10 rows
            print(f"Row {i}: {[cell['text'] for cell in row]}")

        # Test transformation
        transformation = extractor.transform_type2_table_data(target_table['extracted_data'], target_table['headers'])
        print(f"Transformed headers: {transformation['new_headers']}")
        print(f"Number of transformed rows: {len(transformation['transformed_data'])}")

        for i, row in enumerate(transformation['transformed_data'][:5]):  # First 5 transformed rows
            print(f"Transformed Row {i}: {[cell['text'] for cell in row]}")

if __name__ == "__main__":
    test_unh_extraction()