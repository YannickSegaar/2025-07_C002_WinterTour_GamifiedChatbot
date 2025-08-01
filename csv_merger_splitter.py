import pandas as pd
import os
import math

def get_file_size_mb(filename):
    """Get file size in MB."""
    try:
        size_bytes = os.path.getsize(filename)
        size_mb = size_bytes / (1024 * 1024)
        return size_mb
    except:
        return 0

def merge_csv_files(main_csv, classification_csv, output_csv):
    """Merge the main analysis CSV with chatbot classification CSV."""
    try:
        print("🔄 MERGING CSV FILES")
        print("=" * 50)
        
        # Load both CSV files
        print(f"📂 Loading main analysis file: {main_csv}")
        main_df = pd.read_csv(main_csv)
        print(f"   📊 Main file: {len(main_df)} rows, {len(main_df.columns)} columns")
        
        print(f"📂 Loading classification file: {classification_csv}")
        classification_df = pd.read_csv(classification_csv)
        print(f"   📊 Classification file: {len(classification_df)} rows, {len(classification_df.columns)} columns")
        
        # Check for common columns (merge keys)
        common_columns = set(main_df.columns).intersection(set(classification_df.columns))
        print(f"\n🔗 Common columns found: {list(common_columns)}")
        
        # Identify merge key (usually Company Name or index)
        merge_key = None
        potential_keys = ['Company Name', 'Company_Name', 'company_name', 'name']
        
        for key in potential_keys:
            if key in common_columns:
                merge_key = key
                break
        
        if not merge_key:
            print("⚠️  No obvious merge key found. Using index-based merge.")
            # Add index as merge key
            main_df['merge_index'] = main_df.index
            classification_df['merge_index'] = classification_df.index
            merge_key = 'merge_index'
        
        print(f"🔑 Using merge key: {merge_key}")
        
        # Perform the merge
        # Left join to keep all companies from main analysis
        merged_df = main_df.merge(
            classification_df, 
            on=merge_key, 
            how='left',
            suffixes=('', '_classified')
        )
        
        print(f"\n✅ MERGE COMPLETED")
        print(f"📊 Merged file: {len(merged_df)} rows, {len(merged_df.columns)} columns")
        
        # Handle duplicate columns
        duplicate_columns = [col for col in merged_df.columns if col.endswith('_classified')]
        if duplicate_columns:
            print(f"🔄 Handling {len(duplicate_columns)} duplicate columns...")
            
            for dup_col in duplicate_columns:
                original_col = dup_col.replace('_classified', '')
                if original_col in merged_df.columns:
                    # Update original column with classified data where available
                    merged_df[original_col] = merged_df[original_col].fillna(merged_df[dup_col])
                    # Drop the duplicate column
                    merged_df = merged_df.drop(columns=[dup_col])
        
        # Remove merge_index if we added it
        if 'merge_index' in merged_df.columns:
            merged_df = merged_df.drop(columns=['merge_index'])
        
        # Save merged file
        merged_df.to_csv(output_csv, index=False)
        file_size = get_file_size_mb(output_csv)
        
        print(f"\n💾 MERGED FILE SAVED")
        print(f"📁 File: {output_csv}")
        print(f"📏 Size: {file_size:.2f} MB")
        print(f"📊 Final data: {len(merged_df)} companies, {len(merged_df.columns)} columns")
        
        # Analysis of merged data
        print(f"\n📈 MERGED DATA ANALYSIS")
        if 'has_chatbot' in merged_df.columns:
            chatbot_counts = merged_df['has_chatbot'].value_counts()
            print(f"💬 Chatbot analysis:")
            for value, count in chatbot_counts.items():
                print(f"   {value}: {count} companies")
        
        if 'prospect_evaluation' in merged_df.columns:
            prospect_counts = merged_df['prospect_evaluation'].value_counts()
            print(f"🎯 Prospect breakdown:")
            for prospect_type, count in prospect_counts.items():
                print(f"   {prospect_type}: {count} companies")
        
        if 'still_prospect' in merged_df.columns:
            reclassified_counts = merged_df['still_prospect'].value_counts()
            print(f"🔄 Reclassification results:")
            for value, count in reclassified_counts.items():
                print(f"   Still prospect = {value}: {count} companies")
        
        return merged_df, file_size
        
    except Exception as e:
        print(f"❌ Error merging files: {e}")
        import traceback
        traceback.print_exc()
        return None, 0

def split_csv_for_airtable(input_csv, max_size_mb=4.5, output_prefix="airtable_import"):
    """Split CSV into chunks for Airtable import (max 5MB each)."""
    try:
        print(f"\n📋 SPLITTING CSV FOR AIRTABLE IMPORT")
        print("=" * 50)
        
        df = pd.read_csv(input_csv)
        total_size = get_file_size_mb(input_csv)
        
        print(f"📊 Input file: {len(df)} rows, {total_size:.2f} MB")
        print(f"🎯 Target: Max {max_size_mb} MB per file")
        
        if total_size <= max_size_mb:
            print(f"✅ File is already under {max_size_mb} MB - no splitting needed!")
            return [input_csv]
        
        # Calculate number of chunks needed
        num_chunks = math.ceil(total_size / max_size_mb)
        rows_per_chunk = math.ceil(len(df) / num_chunks)
        
        print(f"📈 Will create {num_chunks} files with ~{rows_per_chunk} rows each")
        
        output_files = []
        
        for i in range(num_chunks):
            start_idx = i * rows_per_chunk
            end_idx = min((i + 1) * rows_per_chunk, len(df))
            
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            # Create filename
            chunk_filename = f"{output_prefix}_part_{i+1}_of_{num_chunks}.csv"
            
            # Save chunk
            chunk_df.to_csv(chunk_filename, index=False)
            chunk_size = get_file_size_mb(chunk_filename)
            
            print(f"📁 Created: {chunk_filename}")
            print(f"   📊 Rows: {len(chunk_df)} | Size: {chunk_size:.2f} MB")
            
            # Add metadata to track chunks
            chunk_info = {
                'filename': chunk_filename,
                'rows': len(chunk_df),
                'size_mb': chunk_size,
                'companies': f"Row {start_idx + 1} to {end_idx}"
            }
            
            output_files.append(chunk_info)
        
        print(f"\n✅ SPLITTING COMPLETED")
        print(f"📋 Created {len(output_files)} files for Airtable import")
        print(f"💾 Total data preserved: {sum(f['rows'] for f in output_files)} rows")
        
        return output_files
        
    except Exception as e:
        print(f"❌ Error splitting file: {e}")
        return []

def create_import_instructions(output_files, merged_csv):
    """Create instructions for Airtable import."""
    instructions_file = "AIRTABLE_IMPORT_INSTRUCTIONS.txt"
    
    with open(instructions_file, 'w') as f:
        f.write("🎯 AIRTABLE IMPORT INSTRUCTIONS\n")
        f.write("=" * 50 + "\n\n")
        
        f.write("📋 IMPORT ORDER (Import in this exact order):\n\n")
        
        for i, file_info in enumerate(output_files, 1):
            f.write(f"{i}. {file_info['filename']}\n")
            f.write(f"   📊 Rows: {file_info['rows']} companies\n")
            f.write(f"   📏 Size: {file_info['size_mb']:.2f} MB\n")
            f.write(f"   🏢 Companies: {file_info['companies']}\n\n")
        
        f.write("🔧 AIRTABLE IMPORT SETTINGS:\n")
        f.write("- Import Type: 'CSV'\n")
        f.write("- First row contains field names: YES\n")
        f.write("- Create new table: YES (for first file)\n")
        f.write("- Add to existing table: YES (for subsequent files)\n\n")
        
        f.write("📊 FIELD TYPE RECOMMENDATIONS:\n")
        f.write("- has_chatbot: Single Select (True, False)\n")
        f.write("- has_contact_form: Single Select (True, False)\n")
        f.write("- has_online_booking: Single Select (True, False)\n")
        f.write("- prospect_evaluation: Single Select\n")
        f.write("- analysis_confidence: Single Select (High, Medium, Low)\n")
        f.write("- chatbot_type: Single Select\n")
        f.write("- competitive_threat: Single Select\n")
        f.write("- still_prospect: Single Select (True, False)\n\n")
        
        f.write("⚠️  IMPORTANT NOTES:\n")
        f.write("1. Import files in the exact order listed above\n")
        f.write("2. Use 'Company Name' as the primary field\n")
        f.write("3. Check for duplicates after each import\n")
        f.write("4. Verify total row count matches original data\n\n")
        
        f.write(f"📈 EXPECTED FINAL RESULTS:\n")
        f.write(f"- Total companies: {sum(f['rows'] for f in output_files)}\n")
        f.write(f"- Source file: {merged_csv}\n")
        f.write(f"- Split into: {len(output_files)} files\n")
    
    print(f"📋 Import instructions saved to: {instructions_file}")

def main():
    """Main function for merging and splitting CSV files."""
    print("🔄 CSV MERGER & AIRTABLE SPLITTER")
    print("=" * 60)
    
    # Get file inputs
    main_csv = input("Enter main analysis CSV filename (default: FULL_analysis_results.csv): ").strip() or 'FULL_analysis_results.csv'
    classification_csv = input("Enter chatbot classification CSV filename: ").strip()
    merged_output = input("Enter merged output filename (default: MERGED_analysis_results.csv): ").strip() or 'MERGED_analysis_results.csv'
    
    # Check if files exist
    if not os.path.exists(main_csv):
        print(f"❌ Main CSV file not found: {main_csv}")
        return
    
    if not os.path.exists(classification_csv):
        print(f"❌ Classification CSV file not found: {classification_csv}")
        return
    
    # Step 1: Merge CSV files
    merged_df, file_size = merge_csv_files(main_csv, classification_csv, merged_output)
    
    if merged_df is None:
        print("❌ Merge failed. Stopping.")
        return
    
    # Step 2: Check if splitting is needed
    max_size = 4.5  # MB (slightly under 5MB limit for safety)
    
    if file_size > max_size:
        print(f"\n⚠️  File size ({file_size:.2f} MB) exceeds Airtable limit ({max_size} MB)")
        split_choice = input("Split for Airtable import? (y/N): ").strip().lower()
        
        if split_choice == 'y':
            output_files = split_csv_for_airtable(merged_output, max_size, "airtable_import")
            if output_files:
                create_import_instructions(output_files, merged_output)
        else:
            print("💾 Merged file saved. You'll need to split manually for Airtable.")
    else:
        print(f"✅ Merged file ({file_size:.2f} MB) is ready for direct Airtable import!")
    
    print(f"\n🎉 Process completed!")
    print(f"📁 Merged file: {merged_output}")
    print(f"🎯 Ready for Airtable import!")

if __name__ == "__main__":
    main()