import pandas as pd
import os

def find_company_column(df):
    """Find the company name column in the dataframe."""
    potential_columns = [
        'Company Name', 'company_name', 'Company_Name', 'name', 'Name',
        'company', 'Company', 'business_name', 'Business Name',
        'organization', 'Organization'
    ]
    
    for col in potential_columns:
        if col in df.columns:
            return col
    
    # If no obvious column found, show available columns
    print("❌ No company name column found!")
    print("📋 Available columns:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i}. {col}")
    
    while True:
        try:
            choice = input("\nEnter column number for company names: ").strip()
            col_index = int(choice) - 1
            if 0 <= col_index < len(df.columns):
                return df.columns[col_index]
            else:
                print("❌ Invalid choice. Try again.")
        except ValueError:
            print("❌ Please enter a number.")

def analyze_duplicates(df, key_column=None):
    """Analyze duplicate patterns in the dataframe."""
    print(f"🔍 DUPLICATE ANALYSIS")
    print("=" * 50)
    
    # Find company column if not provided
    if key_column is None or key_column not in df.columns:
        key_column = find_company_column(df)
        if key_column is None:
            return False
    
    print(f"🔑 Using company column: '{key_column}'")
    
    # Count total duplicates
    duplicates = df[df.duplicated(subset=[key_column], keep=False)]
    unique_duplicate_companies = df[df.duplicated(subset=[key_column], keep='first')]
    
    print(f"📊 Total rows: {len(df)}")
    print(f"📊 Unique companies: {df[key_column].nunique()}")
    print(f"❌ Duplicate rows: {len(duplicates)}")
    print(f"🏢 Companies with duplicates: {len(unique_duplicate_companies)}")
    
    if len(duplicates) > 0:
        print(f"\n📋 TOP DUPLICATE COMPANIES:")
        duplicate_counts = df[key_column].value_counts()
        top_duplicates = duplicate_counts[duplicate_counts > 1].head(10)
        
        for company, count in top_duplicates.items():
            print(f"   '{company}': {count} times")
            
        return True, key_column
    else:
        print("✅ No duplicates found!")
        return False, key_column

def smart_deduplicate(df, key_column, priority_columns=None):
    """Smart deduplication that keeps the most complete record."""
    
    if priority_columns is None:
        priority_columns = [
            'analysis_status', 'chatbot_type', 'prospect_evaluation', 
            'pages_analyzed', 'last_analyzed', 'analysis_confidence'
        ]
    
    print(f"\n🧹 SMART DEDUPLICATION")
    print("=" * 50)
    
    # Check which priority columns exist
    existing_priority_cols = [col for col in priority_columns if col in df.columns]
    print(f"📋 Using priority columns: {existing_priority_cols}")
    
    deduplicated_rows = []
    
    # Group by company name
    grouped = df.groupby(key_column)
    
    for company_name, group in grouped:
        if len(group) == 1:
            # No duplicates, keep as is
            deduplicated_rows.append(group.iloc[0])
        else:
            # Multiple records, choose the best one
            print(f"🔄 Processing duplicates for: {company_name} ({len(group)} records)")
            
            # Scoring system to find best record
            best_record = None
            best_score = -1
            
            for idx, record in group.iterrows():
                score = 0
                
                # Score based on completeness
                for col in existing_priority_cols:
                    if pd.notna(record[col]) and str(record[col]).strip() != '':
                        if col == 'analysis_status' and record[col] == 'COMPLETED':
                            score += 10
                        elif col == 'chatbot_type' and record[col] not in ['', 'unknown', 'error']:
                            score += 5
                        elif col == 'pages_analyzed' and pd.notna(record[col]) and record[col] > 0:
                            score += 3
                        elif col == 'analysis_confidence' and record[col] == 'High':
                            score += 2
                        else:
                            score += 1
                
                # Prefer more recent analysis
                if 'last_analyzed' in record and pd.notna(record['last_analyzed']):
                    score += 1
                
                if score > best_score:
                    best_score = score
                    best_record = record
            
            if best_record is not None:
                deduplicated_rows.append(best_record)
                print(f"   ✅ Kept record with score: {best_score}")
            else:
                # Fallback: keep first record
                deduplicated_rows.append(group.iloc[0])
                print(f"   ⚠️  Used fallback (first record)")
    
    # Create new dataframe
    deduplicated_df = pd.DataFrame(deduplicated_rows)
    
    print(f"\n📈 DEDUPLICATION RESULTS:")
    print(f"📊 Original rows: {len(df)}")
    print(f"📊 Deduplicated rows: {len(deduplicated_df)}")
    print(f"❌ Removed duplicates: {len(df) - len(deduplicated_df)}")
    
    return deduplicated_df

def clean_and_split_csv(input_csv, max_size_mb=4.5):
    """Clean duplicates and split for Airtable import."""
    
    print(f"🧹 CSV CLEANER & SPLITTER")
    print("=" * 60)
    
    # Load CSV
    print(f"📂 Loading: {input_csv}")
    df = pd.read_csv(input_csv)
    
    print(f"📋 Available columns in CSV:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i}. {col}")
    
    # Analyze duplicates (will auto-detect company column)
    result = analyze_duplicates(df)
    
    if isinstance(result, tuple):
        has_duplicates, key_column = result
    else:
        has_duplicates = result
        key_column = find_company_column(df)
    
    if has_duplicates:
        # Deduplicate
        clean_df = smart_deduplicate(df, key_column)
        
        # Save clean version
        clean_filename = input_csv.replace('.csv', '_CLEAN.csv')
        clean_df.to_csv(clean_filename, index=False)
        print(f"💾 Clean version saved: {clean_filename}")
        
        # Use clean version for splitting
        df_to_split = clean_df
        base_filename = clean_filename.replace('.csv', '')
    else:
        print("✅ No cleaning needed")
        df_to_split = df
        base_filename = input_csv.replace('.csv', '')
    
    # Check file size and split if needed
    try:
        file_size = os.path.getsize(input_csv) / (1024 * 1024) if has_duplicates else 0
    except:
        file_size = 0
    
    if len(df_to_split) > 500:  # Split if more than 500 rows regardless of size
        print(f"\n📋 SPLITTING FOR AIRTABLE IMPORT")
        print("=" * 50)
        
        # Calculate optimal chunk size
        total_rows = len(df_to_split)
        rows_per_chunk = min(500, max(200, total_rows // 3))  # 200-500 rows per chunk
        
        num_chunks = (total_rows + rows_per_chunk - 1) // rows_per_chunk
        
        print(f"📊 Total rows: {total_rows}")
        print(f"📦 Creating {num_chunks} chunks with ~{rows_per_chunk} rows each")
        
        output_files = []
        
        for i in range(num_chunks):
            start_idx = i * rows_per_chunk
            end_idx = min((i + 1) * rows_per_chunk, total_rows)
            
            chunk_df = df_to_split.iloc[start_idx:end_idx].copy()
            
            # Create clean filename
            chunk_filename = f"airtable_import_CLEAN_part_{i+1}_of_{num_chunks}.csv"
            chunk_df.to_csv(chunk_filename, index=False)
            
            try:
                chunk_size = os.path.getsize(chunk_filename) / (1024 * 1024)
            except:
                chunk_size = 0
            
            print(f"📁 Created: {chunk_filename}")
            print(f"   📊 Rows: {len(chunk_df)} | Size: {chunk_size:.2f} MB")
            
            output_files.append({
                'filename': chunk_filename,
                'rows': len(chunk_df),
                'size_mb': chunk_size,
                'range': f"Companies {start_idx + 1} to {end_idx}"
            })
        
        # Create import instructions
        create_clean_import_instructions(output_files, total_rows)
        
        return output_files
    
    else:
        print(f"✅ File is ready for direct import ({len(df_to_split)} rows)")
        return [{'filename': input_csv, 'rows': len(df_to_split)}]

def create_clean_import_instructions(output_files, total_companies):
    """Create import instructions for clean files."""
    
    instructions_file = "AIRTABLE_IMPORT_INSTRUCTIONS_CLEAN.txt"
    
    with open(instructions_file, 'w') as f:
        f.write("🧹 CLEAN AIRTABLE IMPORT INSTRUCTIONS\n")
        f.write("=" * 50 + "\n\n")
        f.write("✅ DUPLICATES REMOVED - READY FOR IMPORT\n\n")
        
        f.write("📋 IMPORT ORDER (Import in this exact order):\n\n")
        
        for i, file_info in enumerate(output_files, 1):
            f.write(f"{i}. {file_info['filename']}\n")
            f.write(f"   📊 Rows: {file_info['rows']} companies (NO DUPLICATES)\n")
            f.write(f"   📏 Size: {file_info['size_mb']:.2f} MB\n")
            f.write(f"   🏢 Range: {file_info['range']}\n\n")
        
        f.write("🎯 IMPORT SETTINGS:\n")
        f.write("- Import Type: CSV\n")
        f.write("- First row contains headers: YES\n")
        f.write("- Primary field: Company Name\n")
        f.write("- Create new table: YES (first file only)\n")
        f.write("- Add to existing: YES (subsequent files)\n\n")
        
        f.write("✅ VERIFICATION STEPS:\n")
        f.write("1. After importing all files, check total records\n")
        f.write(f"2. Should be exactly {total_companies} companies\n")
        f.write("3. No duplicate company names\n")
        f.write("4. All analysis data preserved\n\n")
        
        f.write("🚨 IMPORTANT:\n")
        f.write("- These files have duplicates REMOVED\n")
        f.write("- Each company appears only ONCE\n")
        f.write("- Most complete data kept for each company\n")
    
    print(f"📋 Clean import instructions: {instructions_file}")

def main():
    """Main deduplication function."""
    print("🧹 CSV DEDUPLICATOR & AIRTABLE SPLITTER")
    print("=" * 60)
    
    # Get input
    input_csv = input("Enter CSV filename to clean: ").strip()
    
    if not os.path.exists(input_csv):
        print(f"❌ File not found: {input_csv}")
        return
    
    # Clean and split
    output_files = clean_and_split_csv(input_csv)
    
    print(f"\n🎉 CLEANING COMPLETED!")
    print(f"📁 Output files: {len(output_files)}")
    print(f"🎯 Ready for Airtable import!")
    
    if len(output_files) > 1:
        print(f"\n💡 NEXT STEPS:")
        print(f"1. Delete the old duplicated CSV files")
        print(f"2. Use the new CLEAN files for Airtable import")
        print(f"3. Follow the CLEAN import instructions")

if __name__ == "__main__":
    main()