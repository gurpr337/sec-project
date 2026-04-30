import sys
import os

# Dynamic path to backend
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from app.services.sec_extractor import SECExtractor

def debug_unh_table():
    # Load the UNH HTML
    with open('unh_full.html', 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Initialize extractor
    extractor = SECExtractor()

    # Parse tables
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

    print("\nFirst 20 rows of extracted_data:")
    for i, row in enumerate(target_table['extracted_data'][:20]):
        row_texts = [cell['text'] for cell in row]
        is_section = any(cell.get('is_section_header', False) for cell in row)
        print(f"Row {i} (section: {is_section}): {row_texts}")

if __name__ == "__main__":
    debug_unh_table()
