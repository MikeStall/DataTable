using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace DataAccess
{
    /// <summary>
    /// Analysis operations on tables, like joins, histogram, dup search, filter, etc.
    /// These handle large tables.
    /// </summary>
    public static class Analyze
    {
        // Zip down file, extract any row that matches the given selection. 
        // Good for streaming over data.
        public static DataTable Where(DataTable table, Func<Row, bool> fpSelector)
        {
            TableWriter writer = new TableWriter(table);

            int count = 0;
            foreach(Row row in table.Rows)
            {   
                bool keep = fpSelector(row);
                if (keep)
                {
                    writer.AddRow(row);
                    count++;
                }                
            }
            return writer.CloseAndGetTable(); 
        }

        // $$$ Clarify - multiple joins (inner, outer, etc)
        // If we have mutable in-memory, then return an in-memory.
        public static MutableDataTable Join(MutableDataTable d1, MutableDataTable d2, string columnName)
        {
            Column c1 = d1.GetColumn(columnName);
            if (c1 == null)
            {
                throw new InvalidOperationException("Missing column");
            }
            Column c2 = d2.GetColumn(columnName);
            if (c2 == null)
            {
                throw new InvalidOperationException("Missing column");
            }

            // Place d1 in first set of columns, and d2 in second set.
            int kColumn = d1.Columns.Length;
            int kTotalColumns = kColumn + d2.Columns.Length;

            // Indices into new table where join columns are.
            int joinColumn1 = Utility.GetColumnIndexFromName(d1.ColumnNames, columnName);
            int joinColumn2 = Utility.GetColumnIndexFromName(d2.ColumnNames, columnName) + kColumn;

            // $$$ could really optimize. Sort both on column and then zip.
            Dictionary<string, int> m1 = GetRowIndex(c1);
            Dictionary<string, int> m2 = GetRowIndex(c2);

            // $$$ column names may not be unique.

            //string[] headers = d1.ColumnNames.Union(d2.ColumnNames).ToArray();
            
            string[] headers = new string[kTotalColumns];
            Array.Copy(d1.ColumnNames.ToArray(), 0, headers, 0, kColumn);
            Array.Copy(d2.ColumnNames.ToArray(), 0, headers, kColumn, kTotalColumns - kColumn);

            string[] values = new string[headers.Length];

            string path = GetTempFileName();
            using (CsvWriter tw = new CsvWriter(path, headers))
            {

                foreach (var kv in m1)
                {
                    Clear(values);                    

                    string key = kv.Key; // join column
                    int r1 = kv.Value;
                    int r2;
                    if (m2.TryGetValue(key, out r2))
                    {
                        // In both.  write out
                        CopyRowIntoArray(values, kColumn, d2, r2);
    
                        m2.Remove(key);
                    }
                    else
                    {
                        // Only in M1. 
                    }

                    CopyRowIntoArray(values, 0, d1, r1);
                    values[joinColumn1] = values[joinColumn2] = key;

                    tw.WriteRow(values);
                }

                // We remove all of M1's items from m2, so M2 is just unique items now. (possibly 0).
                // Tag those onto the end.

                foreach (var kv in m2)
                {
                    int r2 = kv.Value;
                    Clear(values);
                    CopyRowIntoArray(values, kColumn, d2, r2);
                    values[joinColumn1] = values[joinColumn2] = kv.Key;

                    tw.WriteRow(values);
                }

            } // close tw

            MutableDataTable t = Reader.ReadCSV(path);
            DeleteLocalFile(path);

            // Remove duplicate columns.
            t.RemoveColumn(joinColumn2);

            return t;
        }

        static void CopyRowIntoArray(string[] values, int index, MutableDataTable d, int row)
        {
            for (int c = 0; c < d.Columns.Length; c++)
            {
                values[index] = d.Columns[c].Values[row];
                index++;
            }
        }

        static void Clear(string[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = string.Empty;
            }
        }

        static Dictionary<string, int> GetRowIndex(Column c)
        {
            Dictionary<string, int> d = new Dictionary<string, int>();

            for (int row = 0; row < c.Values.Length; row++)
            {
                string x = c.Values[row].ToUpperInvariant();

                // If this add fails, it means the column we're doing a join on has duplicate entries.
                d.Add(x, row); // verifies uniqueness
            }
            return d;
        }


        // Return a sample that's the top N records from a table.
        // This is useful for a large table where you want to quickly get a reference
        public static MutableDataTable SampleTopN(DataTable table, int topN)
        {
            if (topN <= 0)
            {
                throw new ArgumentOutOfRangeException("topN", "sample must be a positive integer");
            }

            TableWriter writer = new TableWriter(table);

            foreach (var row in table.Rows)
            {
                topN--;
                writer.AddRow(row);

                if (topN == 0)
                {
                    // Check topN before the enumeration to avoid pulling a wasted row from the source table
                    break;
                }
            }
                
            return DataTable.New.GetMutableCopy(writer.CloseAndGetTable()); 
        }

        public static int[] GetColumnIndexFromNames(DataTable table, string[] columnNames)
        {
            return Array.ConvertAll(columnNames, columnName => GetColumnIndexFromName(table, columnName));
        }

        // Return 0-based index of column with matching name.
        // throws an exception if not found
        public static int GetColumnIndexFromName(DataTable table, string columnName)
        {
            string[] columnNames = table.ColumnNames.ToArray();
            return Utility.GetColumnIndexFromName(columnNames, columnName);
        }


        // Extract column as a histogram, sorted in descending order by frequency.
        // Return as Tuple because it has type safety, data-table does not. 
        public static Tuple<string, int>[] AsHistogram(DataTable table, string columnName)
        {
            int i = GetColumnIndexFromName(table, columnName);
            return AsHistogram(table, i);
        }
        public static Tuple<string, int>[] AsHistogram(DataTable table, int columnIdx)
        {
            Dictionary<string, int> values = new Dictionary<string, int>();

            //string name = "unknown";
            foreach (Row row in table.Rows)
            {

                string[] parts = row.Values;
                if (columnIdx >= parts.Length)
                {
                    // malformed input file
                    continue;
                }
                string p = parts[columnIdx];

                int count;
                values.TryGetValue(p, out count);
                count++;
                values[p] = count;
            }

            // Get top N?

            var items = from kv in values
                        orderby kv.Value descending
                        select Tuple.Create(kv.Key, kv.Value)
                        ;

            //int N = 10;
            //return items.Take(N).ToArray();
            return items.ToArray();
        }


        // Look down each column and count uniqueness.
        // Returns a datatable of records where:
        //  r1 = column name
        //  r2 = # of unique elements in that column
        // Most common occurences?
        public static MutableDataTable GetColumnValueCounts(DataTable table, int N)
        {
            string[] names = table.ColumnNames.ToArray();
            int count = names.Length;

            MutableDataTable dSummary = new MutableDataTable();
            Column c1 = new Column("column name", count);
            Column c2 = new Column("count", count);

            int kFixed = 2;
            Column[] cAll = new Column[kFixed + N * 2];
            cAll[0] = c1;
            cAll[1] = c2;

            for (int i = 0; i < N; i++)
            {
                cAll[i * 2 + kFixed] = new Column("Top Value " + i, count);
                cAll[i * 2 + 1 + kFixed] = new Column("Top Occurrence " + i, count);
            }
            dSummary.Columns = cAll;

            int columnId = 0;
            foreach (string name in names)
            {
                Tuple<string, int>[] hist = AsHistogram(table, columnId);

                c1.Values[columnId] = name;
                c2.Values[columnId] = hist.Length.ToString();

                for (int i = 0; i < N; i++)
                {
                    if (i >= hist.Length)
                    {
                        break;
                    }
                    cAll[i * 2 + kFixed].Values[columnId] = hist[i].Item1;
                    cAll[i * 2 + 1 + kFixed].Values[columnId] = hist[i].Item2.ToString();
                }

                columnId++;
            }

            return dSummary;
        }

        // Find all rows that have dups for the given columns.
        // This uses a multi-pass algorithm to operate on a large data file.
        public static MutableDataTable SelectDuplicates(DataTable table, params string[] columnNames)
        {
            int[] ci = GetColumnIndexFromNames(table, columnNames);

            // Store on hash keys first. Use hash keys because they're compact and efficient for large data sets
            // But then we do need to handle collisions. 
            HashSet<int> allKeys = new HashSet<int>();
            HashSet<int> possibleDups = new HashSet<int>();

            //
            // Take a first pass and store the hash of each row's unique Key
            //
            foreach (Row row in table.Rows)
            {
                string[] parts = row.Values;
                int hash = CalcHash(parts, ci);

                if (allKeys.Contains(hash))
                {
                    possibleDups.Add(hash);
                }
                else
                {
                    allKeys.Add(hash);
                }
            }
            allKeys = null; // Free up for GC

            //
            // Now take a second pass through the dups.
            //
            Dictionary<string, Row> fullMatch = new Dictionary<string, Row>();

            StringBuilder sb = new StringBuilder();

            string path = GetTempFileName();
            //using (TextWriter tw = new StreamWriter(path))
            using (var writer = new CsvWriter(path, table.ColumnNames))
            {
                foreach (Row row in table.Rows)
                {
                    {
                        string[] parts = row.Values;
                        int hash = CalcHash(parts, ci);
                        if (!possibleDups.Contains(hash))
                        {
                            continue;
                        }

                        // Potential match                    
                        sb.Clear();
                        foreach (int i in ci)
                        {
                            sb.Append(parts[i]);
                            sb.Append(',');
                        }
                        string key = sb.ToString();

                        if (fullMatch.ContainsKey(key))
                        {
                            Row firstLine = fullMatch[key];
                            if (firstLine != null)
                            {
                                writer.WriteRow(firstLine.Values);
                                fullMatch[key] = null;
                            }

                            // Real dup!
                            writer.WriteRow(row.Values);
                        }
                        else
                        {
                            fullMatch[key] = row;
                        }
                    }
                } // reader
            } // writer


            MutableDataTable d = Reader.ReadCSV(path);
            DeleteLocalFile(path);
            return d;
        }

        // Helper for finding duplicates.
        private static int CalcHash(string[] parts, int[] ci)
        {
            int h = 0;
            foreach (int i in ci)
            {
                h += parts[i].GetHashCode();
            }
            return h;
        }

        // For azure, must be able to override.
        public static Func<string> GetTempFileName = System.IO.Path.GetTempFileName;

        private static void DeleteLocalFile(string file)
        {
            try
            {
                System.IO.File.Delete(file);
            }
            catch
            {
                // Not fatal.
            }
        }
    }
}
