import os
import pandas as pd
import re

# Directories for input, output, and reports
data_dir = 'DATA'
out_dir = 'OUT'
report_dir = 'REPORT'

# Ensure output directories exist
os.makedirs(out_dir, exist_ok=True)
os.makedirs(report_dir, exist_ok=True)

# File paths
journal_data_file = os.path.join(data_dir, 'journal_data.txt')
journals_csv_file = os.path.join(data_dir, 'journals.csv')
merged_output_file = os.path.join(out_dir, 'merged_journal_data.csv')
ext_info_output_file = os.path.join(out_dir, 'journal_data_ext_info.txt')
relational_output_file = os.path.join(out_dir, 'journal_issues_relational.csv')
report_file_path = os.path.join(report_dir, 'journal_data_report.txt')

# Patterns for extracting data
issn_pattern = re.compile(r'ISSN\s*\d{4}-[\dXx]{4}')
invbr_pattern = re.compile(r'inv\.? br\.?\s*([^|]+)', re.IGNORECASE)
location_pattern = re.compile(r'\*{2,}\s*(.*?)\s*(?=\s{2,}|\||$)')
volume_issue_pattern = re.compile(r'(\d+)\s*\((\d{4})\)(?:\s*[-–]\s*(\d+)\s*\((\d{4})\))?')
issue_range_pattern = re.compile(r'No\.\s*(\d+)(?:[-–]\s*(\d+))?')

# Step 1: Read and process the journal_data.txt file
data = []

with open(journal_data_file, 'r', encoding='utf-8') as file:
    for line in file:
        title, *rest = line.strip().split('|', 1)
        journal_ext_info = rest[0] if rest else ''
        
        # Extract ISSN
        issn_match = issn_pattern.search(journal_ext_info)
        issn = issn_match.group(0).replace('ISSN', '').strip() if issn_match else None
        if issn_match:
            journal_ext_info = journal_ext_info.replace(issn_match.group(0), '').strip()
        
        # Extract INVBR
        invbr_match = invbr_pattern.search(journal_ext_info)
        invbr = invbr_match.group(1).strip() if invbr_match else None
        if invbr_match:
            journal_ext_info = re.sub(invbr_pattern, '', journal_ext_info).strip()
        
        # Extract Location
        location_match = location_pattern.search(journal_ext_info)
        location = location_match.group(1).strip() if location_match else None
        if location_match:
            journal_ext_info = re.sub(location_pattern, '', journal_ext_info).strip()
        
        # Clean up the Journal_Ext_Info by removing leading, trailing, and consecutive pipes
        journal_ext_info = re.sub(r'\|+', '|', journal_ext_info).strip('|')
        
        journal_autofix_title = title.split(',', 1)[0].strip() if title else None
        
        data.append({
            'Journals_Data_Title': title,
            'journal_autofix_title': journal_autofix_title,
            'Journals_Data_ISSN': issn,
            'Journals_Data_INVBR': invbr,
            'Journals_Data_Location': location,
            'Journals_Data_Journal_Ext_Info': journal_ext_info,
            'Source_Line': line.strip()
        })

df_journal_data = pd.DataFrame(data)

# Step 2: Merge with journals.csv
df_journals = pd.read_csv(journals_csv_file, quotechar='"', skipinitialspace=True)
df_journals.rename(columns={
    'issn': 'CLEAN_ISSN',
    'title': 'CLEAN_TITLE',
    'title_ext': 'CLEAN_TITLE_ext',
    'publisher': 'CLEAN_PUBLISHER'
}, inplace=True)

merged_df = pd.merge(df_journal_data, df_journals, how='left', left_on='Journals_Data_ISSN', right_on='CLEAN_ISSN')
merged_records_count = merged_df['CLEAN_ISSN'].notnull().sum()

# Step 3: Save the merged data to a new CSV file in the OUT directory
merged_df.to_csv(merged_output_file, index=False, encoding='utf-8')

# Save only the content of Journals_Data_Journal_Ext_Info to a separate file
merged_df['Journals_Data_Journal_Ext_Info'].to_csv(ext_info_output_file, index=False, header=False, encoding='utf-8')

# Step 4: Parse Journal_Ext_Info for relational data
relational_data = []

for _, row in merged_df.iterrows():
    issn = row['Journals_Data_ISSN']
    ext_info = row['Journals_Data_Journal_Ext_Info']
    signature = None
    annotation = None
    
    if 'Sgn.' in ext_info:
        ext_info, signature = ext_info.split('Sgn.', 1)
        signature = signature.strip()
    
    if '*' in ext_info:
        ext_info, annotation = ext_info.split('*', 1)
        annotation = annotation.strip()
    
    # Extract volume and issue ranges
    for match in volume_issue_pattern.finditer(ext_info):
        vol_start, year_start, vol_end, year_end = match.groups()
        relational_data.append({
            'ISSN': issn,
            'Volume_Start': vol_start,
            'Year_Start': year_start,
            'Volume_End': vol_end,
            'Year_End': year_end,
            'Issue_Start': None,
            'Issue_End': None,
            'Supplement': 'No',
            'Signature': signature,
            'Annotation': annotation
        })
    
    # Extract specific issues (e.g., No. 1-2)
    for match in issue_range_pattern.finditer(ext_info):
        issue_start, issue_end = match.groups()
        relational_data.append({
            'ISSN': issn,
            'Volume_Start': None,
            'Year_Start': None,
            'Volume_End': None,
            'Year_End': None,
            'Issue_Start': issue_start,
            'Issue_End': issue_end,
            'Supplement': 'No',
            'Signature': signature,
            'Annotation': annotation
        })

# Convert relational data to DataFrame and save
df_relational = pd.DataFrame(relational_data)
df_relational.to_csv(relational_output_file, index=False, encoding='utf-8')

# Step 5: Generate the report
total_records = len(df_journal_data)
records_with_issn = df_journal_data['Journals_Data_ISSN'].notnull().sum()
records_without_issn = df_journal_data['Journals_Data_ISSN'].isnull().sum()
duplicate_issns = df_journal_data['Journals_Data_ISSN'].dropna()
duplicate_issns = duplicate_issns[duplicate_issns.duplicated()].unique()

# Create the report file in the REPORT directory
with open(report_file_path, 'w') as report_file:
    report_file.write("##### Report Summary #####\n")
    report_file.write(f"Total number of records: {total_records}\n")
    report_file.write(f"Number of records where ISSN is found: {records_with_issn}\n")
    report_file.write(f"Number of records without ISSN: {records_without_issn}\n")
    report_file.write(f"Number of duplicate ISSNs: {len(duplicate_issns)}\n")
    report_file.write(f"Duplicate ISSNs: {', '.join(duplicate_issns)}\n\n")
    report_file.write(f"Number of records found and merged from CSV: {merged_records_count}\n\n")
    
    report_file.write("##### 5 Random Records #####\n\n")
    random_records = df_journal_data.sample(n=5)
    for _, row in random_records.iterrows():
        for col in df_journal_data.columns[:-1]:  # Exclude 'Source_Line' from this part
            report_file.write(f"{col}: {row[col]}\n")
        report_file.write("\n\n\n")
    
    report_file.write("##### 3 Random Records Without ISSN #####\n\n")
    no_issn_records = df_journal_data[df_journal_data['Journals_Data_ISSN'].isnull()].sample(n=3)
    for _, row in no_issn_records.iterrows():
        report_file.write(f"Source Data: {row['Source_Line']}\n")
        report_file.write("\n\n\n")
    
    report_file.write("##### 3 Random Merged Records #####\n\n")
    matched_records = merged_df[merged_df['CLEAN_ISSN'].notnull()].sample(n=3)
    for _, row in matched_records.iterrows():
        for col in merged_df.columns:
            report_file.write(f"{col}: {row[col]}\n")
        report_file.write("\n\n\n")

print(f"Merged data saved successfully to '{merged_output_file}'")
print(f"Journals_Data_Journal_Ext_Info saved to '{ext_info_output_file}'")
print(f"Relational journal issue data saved to '{relational_output_file}'")
print(f"Report generated as '{report_file_path}'")

