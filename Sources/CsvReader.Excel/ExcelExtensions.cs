using System;
using System.Collections.Generic;
using System.Linq;

using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.IO;
using DocumentFormat.OpenXml;

namespace DataAccess
{
    public static class ExcelExtensions
    {
        /// <summary>
        /// Reads the first worksheet in the .xlsx file and returns it. This only supports .xlsx files (Office 2007, with open xml standard) 
        /// and not .xls files (which had a closed file format that required COM). 
        /// Also supports reading .csv files.
        /// This is safe to use on a server. 
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="filename">filename </param>
        /// <returns>table for the first sheet in the workbook. Table's name is the sheet name.</returns>
        public static MutableDataTable ReadExcel(this DataTableBuilder builder, string filename)
        {
            // For convenience
            if (filename.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            {
                return builder.ReadCsv(filename);
            }

            using (Stream input = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                return builder.ReadExcel(input);
            }            
        }

        /// <summary>
        /// Reads the first worksheet in the .xlsx file and returns it. This only supports .xlsx files (Office 2007, with open xml standard) 
        /// and not .xls files (which had a closed file format that required COM). 
        /// This is safe to use on a server. 
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="input">stream to read file from</param>
        /// <returns>table for the first sheet in the workbook. Table's name is the sheet name.</returns>
        public static MutableDataTable ReadExcel(this DataTableBuilder builder, Stream input)
        {
            // See http://msdn.microsoft.com/en-us/library/hh298534.aspx
            using (SpreadsheetDocument document = SpreadsheetDocument.Open(input, isEditable: false))
            {
                // Retrieve a reference to the workbook part.
                WorkbookPart wbPart = document.WorkbookPart;

                // Get the first sheet
                foreach (Sheet sheet in wbPart.Workbook.Descendants<Sheet>())
                {
                    MutableDataTable dt = ReadSheet(wbPart, sheet);
                    if (dt != null)
                    {
                        return dt;
                    }
                }
            }

            throw new InvalidOperationException("Excel file is either empty or does not have a valid table in it.");
        }

        /// <summary>
        /// Reads all sheets in the excel workbook and returns as a ordered collection of data tables.
        /// </summary>
        /// <param name="builder">placeholder</param>
        /// <param name="filename">excel file to load</param>
        /// <returns>Ordered collection of tables corresponding to non-empty sheets. Table name corresponds to sheet name.</returns>
        public static IList<MutableDataTable> ReadExcelAllSheets(this DataTableBuilder builder, string filename)
        {
            using (Stream input = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                return builder.ReadExcelAllSheets(input);
            }     
        }

        /// <summary>
        /// Reads all sheets in the excel workbook and returns as a ordered collection of data tables.
        /// </summary>
        /// <param name="builder">placeholder</param>
        /// <param name="input">stream to read from</param>
        /// <returns>Ordered collection of tables corresponding to non-empty sheets. Table name corresponds to sheet name.</returns>
        public static IList<MutableDataTable> ReadExcelAllSheets(this DataTableBuilder builder, Stream input)
        {
            List<MutableDataTable> list = new List<MutableDataTable>();

            // See http://msdn.microsoft.com/en-us/library/hh298534.aspx
            using (SpreadsheetDocument document = SpreadsheetDocument.Open(input, isEditable: false))
            {
                // Retrieve a reference to the workbook part.
                WorkbookPart wbPart = document.WorkbookPart;

                // Get the first sheet
                foreach (Sheet sheet in wbPart.Workbook.Descendants<Sheet>())
                {
                    MutableDataTable dt = ReadSheet(wbPart, sheet);
                    if (dt != null)
                    {
                        list.Add(dt);
                    }
                }
            }

            return list;
        }

        // Read the excel sheet from the workbook and return as a data table.
        // Return null if sheet is empty.
        private static MutableDataTable ReadSheet(WorkbookPart wbPart, Sheet sheet)
        {
            string sheetName = sheet.Name.Value;

            // Retrieve a reference to the worksheet part.
            WorksheetPart wsPart = (WorksheetPart)(wbPart.GetPartById(sheet.Id));

            IEnumerable<Cell> cells = wsPart.Worksheet.Descendants<Cell>();

            Dictionary2d<int, int, string> vals = new Dictionary2d<int, int, string>();
            
            // Retrieve a cached list of shared strings of this workbook to be used by all cell references
            IList<OpenXmlElement> sharedStrings = wbPart.GetPartsOfType<SharedStringTablePart>().Select(sharedString => sharedString.SharedStringTable.OfType<OpenXmlElement>().ToList()).FirstOrDefault();
            
            foreach (Cell c in cells)
            {
                var val = CellToText(wbPart, c, sharedStrings);
                var loc = c.CellReference;
                var loc2 = ParseRef(loc);

                int columnId = loc2.Item1;
                int rowId = loc2.Item2;
                vals[rowId, columnId] = val;
            }
            
            sharedStrings.Clear();

            if (vals.Count > 0)
            {
                MutableDataTable dt = ToTable(vals);
                dt.Name = sheetName;

                return dt;
            }
            return null;
        }

        // skip access
        private static MutableDataTable ToTable<TValue>(Dictionary2d<int, int, TValue> dict)
        {
            // TKey1 is rows, TKey2 is values.
            MutableDataTable d = new MutableDataTable();

            var rows = dict.Key1;
            int count = rows.Count() - 1;

            // Set columns
            var columns = dict.Key2.ToArray();
            {
                Column[] cs = new Column[columns.Length];                
                for (int ic = 0; ic < columns.Length; ic++)
                {
                    // fix for empty column name
                    string columnName = dict[0, columns[ic]] == null ? string.Empty : dict[0, columns[ic]].ToString(); ;
                    cs[ic] = new Column(columnName, count);
                }
                d.Columns = cs;
            }

            // Add rows
            int i = 0;
            foreach (var row in rows)
            {
                i++;
                if (i == 1)
                {                    
                    continue; // skip 1st row, header
                }
                for (int ic = 0; ic < columns.Length; ic++)
                {
                    var value = dict[row, columns[ic]];
                    string s = (value == null) ? string.Empty : value.ToString();
                    d.Columns[ic].Values[i - 2] = s;
                }
            }

            return d;
        }

        // This function from:
        // http://msdn.microsoft.com/en-us/library/hh298534.aspx
        static string CellToText(WorkbookPart wbPart, Cell theCell, IList<OpenXmlElement> sharedStrings)
        {
            // If the cell does not exist, return an empty string.
            if (theCell == null)
            {
                return string.Empty;
            }

            string value = theCell.InnerText;

            // If the cell represents an integer number, you are done. 
            // For dates, this code returns the serialized value that 
            // represents the date. The code handles strings and 
            // Booleans individually. For shared strings, the code 
            // looks up the corresponding value in the shared string 
            // table. For Booleans, the code converts the value into 
            // the words TRUE or FALSE.
            if (theCell.DataType != null)
            {
                switch (theCell.DataType.Value)
                {
                    case CellValues.SharedString:

                        // If the shared string table is missing, something 
                        // is wrong. Return the index that is in
                        // the cell. Otherwise, look up the correct text in 
                        // the table.
                        if (sharedStrings != null)
                        {
                            return sharedStrings[int.Parse(value)].InnerText;
                        }
                        break;

                    case CellValues.Boolean:
                        switch (value)
                        {
                            case "0":
                                return "FALSE";
                            default:
                                return "TRUE";
                        }
                } // end switch
            }

            // InnerText will show Table formulas. We want the actual computed value.  
            if (theCell.CellValue == null)
            {
                // may happen if a cell is empty
                return string.Empty;
            }

            return theCell.CellValue.Text;

        }

        // Parse ref to loc, 
        // Returns 0-based values (column, row)
        // "B3" --> (), "AA32" --> (1+26
        private static Tuple<int, int> ParseRef(string loc)
        {
            int column = 0;

            for (int idx = 0; idx < loc.Length; idx++)
            {
                char ch = loc[idx];
                int val = ConvertLetter(ch);

                if (val < 0)
                {
                    // end of letter portion. Rest should be a number.
                    string rest = loc.Substring(idx);
                    int row = int.Parse(rest);
                    return System.Tuple.Create(column - 1, row - 1);
                }
                else
                {
                    column *= 26;
                    column += (val + 1);
                }

            }
            // Error! there was no row value.
            throw new InvalidOperationException("illegal location value:" + loc);
        }

        private static int ConvertLetter(char ch)
        {
            if (ch >= 'A' && ch <= 'Z')
            {
                return ch - 'A';
            }
            if (ch >= 'a' && ch <= 'z')
            {
                return ch - 'a';
            }
            return -1;
        }
    }
  
}
