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
        public static int[] GetColumnIndexFromNames(DataTableReference table, string[] columnNames)
        {
            return Array.ConvertAll(columnNames, columnName => GetColumnIndexFromName(table, columnName));
        }

        // Return 0-based index of column with matching name.
        // throws an exception if not found
        public static int GetColumnIndexFromName(DataTableReference table, string columnName)
        {
            string[] columnNames = table.ColumnNames.ToArray();
            return Utility.GetColumnIndexFromName(columnNames, columnName);
        }


        // Extract column as a histogram, sorted in descending order by frequency.
        // Return as Tuple because it has type safety, data-table does not. 
        public static Tuple<string, int>[] AsHistogram(DataTableReference table, string columnName)
        {
            int i = GetColumnIndexFromName(table, columnName);
            return AsHistogram(table, i);
        }
        public static Tuple<string, int>[] AsHistogram(DataTableReference table, int columnIdx)
        {
            Dictionary<string, int> values = new Dictionary<string, int>();

            //string name = "unknown";
            foreach(Row row in table.Rows)
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
        public static DataTable GetColumnValueCounts(DataTableReference table, int N)
        {
            string[] names = table.ColumnNames.ToArray();
            int count = names.Length;

            DataTable dSummary = new DataTable();
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
        public static DataTable SelectDuplicates(DataTableReference table, params string[] columnNames)
        {
            int[] ci = GetColumnIndexFromNames(table, columnNames);

            // Store on hash keys first. Use hash keys because they're compact and efficient for large data sets
            // But then we do need to handle collisions. 
            HashSet<int> allKeys = new HashSet<int>();
            HashSet<int> possibleDups = new HashSet<int>();

            char chSeparator;
            //
            // Take a first pass and store the hash of each row's unique Key
            //
            foreach(Row row in table.Rows)            
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
                foreach(Row row in table.Rows)
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


            DataTable d = Reader.ReadCSV(path);            
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
