import pandas as pd
import sys
from collections import Counter

def analyze_airtable_duplicates(airtable_csv_path):
    """
    Task 1: Analyze Airtable CSV for duplicate Apollo Lead IDs
    
    Args:
        airtable_csv_path (str): Path to the Airtable CSV export
    
    Returns:
        tuple: (duplicate_ids, duplicate_report)
    """
    print("=== TASK 1: ANALYZING AIRTABLE DUPLICATES ===")
    
    try:
        # Read the Airtable CSV
        df_airtable = pd.read_csv(airtable_csv_path)
        print(f"Loaded Airtable CSV with {len(df_airtable)} records")
        
        # Check if apollo_lead_id column exists
        if 'apollo_lead_id' not in df_airtable.columns:
            print("ERROR: 'apollo_lead_id' column not found in Airtable CSV")
            print("Available columns:", list(df_airtable.columns))
            return None, None
        
        # Remove rows where apollo_lead_id is null/empty
        df_clean = df_airtable.dropna(subset=['apollo_lead_id'])
        df_clean = df_clean[df_clean['apollo_lead_id'].astype(str).str.strip() != '']
        
        print(f"Records with valid Apollo Lead IDs: {len(df_clean)}")
        
        # Find duplicates
        apollo_id_counts = df_clean['apollo_lead_id'].value_counts()
        duplicates = apollo_id_counts[apollo_id_counts > 1]
        
        if len(duplicates) == 0:
            print("✅ No duplicate Apollo Lead IDs found!")
            return [], "No duplicates found"
        
        print(f"🚨 Found {len(duplicates)} Apollo Lead IDs with duplicates:")
        print(f"Total duplicate records: {duplicates.sum() - len(duplicates)}")
        
        duplicate_report = []
        duplicate_ids = []
        
        for apollo_id, count in duplicates.items():
            duplicate_ids.append(apollo_id)
            duplicate_rows = df_clean[df_clean['apollo_lead_id'] == apollo_id]
            
            print(f"\nDuplicate Apollo Lead ID: {apollo_id} (appears {count} times)")
            duplicate_report.append(f"Apollo Lead ID: {apollo_id} - {count} occurrences")
            
            # Show details of duplicate records
            for idx, row in duplicate_rows.iterrows():
                name = row.get('full_name', 'N/A')
                email = row.get('email_address', 'N/A')
                record_id = row.get('record_id', 'N/A')
                print(f"  - Row {idx}: {name} | {email} | Record ID: {record_id}")
                duplicate_report.append(f"  Row {idx}: {name} | {email} | Record ID: {record_id}")
        
        return duplicate_ids, duplicate_report
        
    except Exception as e:
        print(f"Error analyzing Airtable duplicates: {e}")
        return None, None

def compare_apollo_datasets(airtable_csv_path, apollo_csv_path):
    """
    Task 2: Compare Airtable and Apollo datasets to find missing records
    
    Args:
        airtable_csv_path (str): Path to the Airtable CSV export
        apollo_csv_path (str): Path to the Apollo CSV export
    
    Returns:
        tuple: (missing_ids, comparison_report)
    """
    print("\n=== TASK 2: COMPARING APOLLO DATASETS ===")
    
    try:
        # Read both CSV files
        df_airtable = pd.read_csv(airtable_csv_path)
        df_apollo = pd.read_csv(apollo_csv_path)
        
        print(f"Airtable records: {len(df_airtable)}")
        print(f"Apollo records: {len(df_apollo)}")
        
        # Check required columns
        if 'apollo_lead_id' not in df_airtable.columns:
            print("ERROR: 'apollo_lead_id' column not found in Airtable CSV")
            return None, None
            
        if 'id' not in df_apollo.columns:
            print("ERROR: 'id' column not found in Apollo CSV")
            print("Available columns:", list(df_apollo.columns))
            return None, None
        
        # Get clean sets of IDs
        airtable_ids = set(df_airtable.dropna(subset=['apollo_lead_id'])['apollo_lead_id'].astype(str).str.strip())
        apollo_ids = set(df_apollo.dropna(subset=['id'])['id'].astype(str).str.strip())
        
        # Remove empty strings
        airtable_ids = {id for id in airtable_ids if id != ''}
        apollo_ids = {id for id in apollo_ids if id != ''}
        
        print(f"Valid Airtable Apollo Lead IDs: {len(airtable_ids)}")
        print(f"Valid Apollo IDs: {len(apollo_ids)}")
        
        # Find missing records
        missing_in_airtable = apollo_ids - airtable_ids
        extra_in_airtable = airtable_ids - apollo_ids
        
        print(f"\n📊 COMPARISON RESULTS:")
        print(f"Missing in Airtable: {len(missing_in_airtable)} records")
        print(f"Extra in Airtable (not in Apollo): {len(extra_in_airtable)} records")
        print(f"Successfully imported: {len(airtable_ids & apollo_ids)} records")
        
        comparison_report = [
            f"Comparison Results:",
            f"- Total Apollo records: {len(apollo_ids)}",
            f"- Total Airtable records: {len(airtable_ids)}",
            f"- Missing in Airtable: {len(missing_in_airtable)}",
            f"- Extra in Airtable: {len(extra_in_airtable)}",
            f"- Successfully imported: {len(airtable_ids & apollo_ids)}"
        ]
        
        if missing_in_airtable:
            print(f"\n🚨 MISSING APOLLO LEAD IDs (not in Airtable):")
            comparison_report.append("\nMissing Apollo Lead IDs:")
            
            for missing_id in sorted(missing_in_airtable):
                # Find details from Apollo CSV
                apollo_record = df_apollo[df_apollo['id'] == missing_id]
                if not apollo_record.empty:
                    name = apollo_record.iloc[0].get('name', 'N/A')
                    email = apollo_record.iloc[0].get('email', 'N/A')
                    title = apollo_record.iloc[0].get('title', 'N/A')
                    print(f"  {missing_id} - {name} | {email} | {title}")
                    comparison_report.append(f"  {missing_id} - {name} | {email} | {title}")
                else:
                    print(f"  {missing_id} - Record not found in Apollo data")
                    comparison_report.append(f"  {missing_id} - Record not found in Apollo data")
        
        if extra_in_airtable:
            print(f"\n⚠️  EXTRA IN AIRTABLE (not in current Apollo dataset):")
            comparison_report.append("\nExtra in Airtable (not in Apollo):")
            for extra_id in sorted(extra_in_airtable):
                print(f"  {extra_id}")
                comparison_report.append(f"  {extra_id}")
        
        return list(missing_in_airtable), comparison_report
        
    except Exception as e:
        print(f"Error comparing Apollo datasets: {e}")
        return None, None

def save_results_to_file(duplicate_ids, duplicate_report, missing_ids, comparison_report):
    """Save analysis results to text files"""
    try:
        # Save duplicate analysis
        if duplicate_ids is not None:
            with open('duplicate_analysis.txt', 'w') as f:
                f.write("DUPLICATE APOLLO LEAD IDs ANALYSIS\n")
                f.write("=" * 50 + "\n\n")
                if duplicate_report:
                    for line in duplicate_report:
                        f.write(line + "\n")
                else:
                    f.write("No duplicates found.\n")
            
            with open('duplicate_apollo_ids.txt', 'w') as f:
                f.write("DUPLICATE APOLLO LEAD IDs LIST\n")
                f.write("=" * 40 + "\n")
                for duplicate_id in duplicate_ids:
                    f.write(duplicate_id + "\n")
            
            print(f"\n📁 Duplicate analysis saved to: duplicate_analysis.txt")
            print(f"📁 Duplicate IDs list saved to: duplicate_apollo_ids.txt")
        
        # Save missing records analysis
        if missing_ids is not None:
            with open('missing_records_analysis.txt', 'w') as f:
                f.write("MISSING RECORDS ANALYSIS\n")
                f.write("=" * 50 + "\n\n")
                if comparison_report:
                    for line in comparison_report:
                        f.write(line + "\n")
            
            with open('missing_apollo_ids.txt', 'w') as f:
                f.write("MISSING APOLLO LEAD IDs LIST\n")
                f.write("=" * 40 + "\n")
                for missing_id in missing_ids:
                    f.write(missing_id + "\n")
            
            print(f"📁 Missing records analysis saved to: missing_records_analysis.txt")
            print(f"📁 Missing IDs list saved to: missing_apollo_ids.txt")
        
    except Exception as e:
        print(f"Error saving results: {e}")

def main():
    """Main function to run both analysis tasks"""
    print("🚀 APOLLO LEAD DATA ANALYZER")
    print("=" * 50)
    
    # Check command line arguments
    if len(sys.argv) != 3:
        print("Usage: python apollo_analyzer.py <airtable_csv> <apollo_csv>")
        print("\nExample:")
        print("python apollo_analyzer.py airtable_export.csv apollo_dataset.csv")
        print("\nExpected CSV structures:")
        print("- Airtable CSV should have 'apollo_lead_id' column")
        print("- Apollo CSV should have 'id' column")
        return
    
    airtable_csv = sys.argv[1]
    apollo_csv = sys.argv[2]
    
    # Task 1: Check for duplicates in Airtable
    duplicate_ids, duplicate_report = analyze_airtable_duplicates(airtable_csv)
    
    # Task 2: Compare datasets to find missing records
    missing_ids, comparison_report = compare_apollo_datasets(airtable_csv, apollo_csv)
    
    # Save results to files
    save_results_to_file(duplicate_ids, duplicate_report, missing_ids, comparison_report)
    
    print("\n✅ Analysis complete!")

if __name__ == "__main__":
    main()