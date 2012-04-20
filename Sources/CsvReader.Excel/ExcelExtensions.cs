using System;
using System.Collections.Generic;
using System.Linq;

using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;

namespace DataAccess
{
    public static class ExcelExtensions
    {
        // Reads Excel. 
        // Returns set of worksheets. An excel file can have multiple worksheets. Each worksheet is a CSV file. 
        public static IDictionary<string, MutableDataTable> ReadExcel(this DataTableBuilder builder, string filename)
        {
            Dictionary<string, MutableDataTable> tables = new System.Collections.Generic.Dictionary<string, MutableDataTable>();

            // See http://msdn.microsoft.com/en-us/library/hh298534.aspx
            using (SpreadsheetDocument document = SpreadsheetDocument.Open(filename, false))
            {
                // Retrieve a reference to the workbook part.
                WorkbookPart wbPart = document.WorkbookPart;

                // Get the first sheet
                foreach (Sheet sheet in wbPart.Workbook.Descendants<Sheet>())
                {
                    string sheetName = sheet.Name.Value;

                    // Retrieve a reference to the worksheet part.
                    WorksheetPart wsPart = (WorksheetPart)(wbPart.GetPartById(sheet.Id));

                    IEnumerable<Cell> cells = wsPart.Worksheet.Descendants<Cell>();

                    Dictionary2d<int, int, string> vals = new Dictionary2d<int, int, string>();
                    foreach (Cell c in cells)
                    {
                        var val = CellToText(wbPart, c);
                        var loc = c.CellReference;
                        var loc2 = ParseRef(loc);
                                          
                        //Console.WriteLine("{0},{1}={2}", loc2.Item1, loc2.Item2, val);
                        int columnId = loc2.Item1;
                        int rowId = loc2.Item2;
                        vals[rowId, columnId] = val;                        
                    }

                    if (vals.Count > 0)
                    {
                        //MutableDataTable dt = DataTable.New.From2dDictionary(vals);
                        MutableDataTable dt = ToTable(vals);
                        tables[sheetName] = dt;
                    }
                }
            }

            return tables;
        }

        // skip access
        internal static MutableDataTable ToTable<TValue>(Dictionary2d<int, int, TValue> dict)
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
                    string columnName = dict[0, columns[ic]].ToString(); ;
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
        static string CellToText(WorkbookPart wbPart, Cell theCell)
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

                        // For shared strings, look up the value in the
                        // shared strings table.
                        var stringTable =
                            wbPart.GetPartsOfType<SharedStringTablePart>()
                            .FirstOrDefault();

                        // If the shared string table is missing, something 
                        // is wrong. Return the index that is in
                        // the cell. Otherwise, look up the correct text in 
                        // the table.
                        if (stringTable != null)
                        {
                            return stringTable.SharedStringTable.ElementAt(int.Parse(value)).InnerText;
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
            return theCell.CellValue.Text;

        }

        // Parse ref to loc, 
        // Returns 0-based values (column, row)
        // "B3" --> (), "AA32" --> (1+26
        static Tuple<int, int> ParseRef(string loc)
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

        static int ConvertLetter(char ch)
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
