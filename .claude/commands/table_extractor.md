---

description : Extract tables from the mentioned folder and pdf name. In pdf name search for mentioned keyword and extract tables from that pdf with best method. If the year has excel files then give priority to excel and extract tables and continue with pdf for the years which has no excel files.
argument-hint: folder name | year (if not specified then all years) |table number(mention table number in pdf name, if different table numbers are there for different years then mention all table numbers separated by comma) | method | (lattice/stream/auto) | output folder name | python file path
---

## context

Parse $Arguments to extract tables from the mentioned folder and pdf name. In pdf name search for mentioned keyword and extract tables from that pdf with best method.

- [folder name] : Folder name where pdfs are stored
- [year] : Year of the pdfs to search (if not specified then all years)
- [table number] : Table number to extract (if different table numbers are there for different years then mention all table numbers separated by comma Ex (2.2 for ( 2003 - 2015), 1.4 for (2016 - 2023)))
- [method] : Method to extract tables
- [output folder name] : Folder name where output csvs are stored
- [python file path] : Python file path to store the code

## Task

Extract tables from the mentioned [folder name] and pdf name. In pdf search for mentioned [table number] and extract tables from that pdf with best [method].

**For each year folder, apply the Excel-first rule:**
1. Check if a matching Excel file (`.xlsx` or `.xls`) exists for that year — if found, extract from Excel using `pd.read_excel()` and skip PDF extraction for that year entirely.
2. If no Excel file exists, proceed with PDF extraction using camelot as usual.

create python file in [python file path]
create tables in data/interim/[output folder name]

use table area as ["0","800","800","0"]
for complex tables consider the code in the following file "projects/ncrb/src/data/crime_in_india/crimes_against_children/state/crimes_against_children_head_wise/create_interim.py" 

for simple tables consider the code in the following file "projects/ncrb/src/data/suicides_in_india/educational_status/create_interim_dataset.py"

you have to save file in the same pattern that is used in the mentioned files "projects/ncrb/src/data/suicides_in_india/educational_status/create_interim_dataset.py"

Save output files in the same manner how the code looks in the mentioned files "projects/ncrb/src/data/suicides_in_india/educational_status/create_interim_dataset.py"

## output 

output files should be in the following format

if pdf contains multiple tables then save each table in a separate file in data/interim/[output folder name]/[extracted table number]/output.csv 

if pdf contains single table then save that table in a file in data/interim/[output folder name]/output.csv


## Review work 

-- ** Invoke table_extractor subagent ** to review the tables extracted from the pdf and check if the tables are extracted correctly. Check the both interim files and python script files are stored in the correct format and location and implement the changes if required. If tables are extracted correctly then move to the next step.If not then re-extract the tables.
-- Iterate on the review process when needed.
