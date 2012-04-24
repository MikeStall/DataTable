using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace DataAccess
{
    /// <summary>
    /// Exception thrown on illegal user operation.
    /// </summary>
    [Serializable]
    public class AssertException : Exception
    {
        internal AssertException(string message)
            : base(message)
        { }
    }

    internal static class Utility
    {
        // Helper for Dictionaries. 
        public static TValue GetOrCreate<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey lookup) where TValue : new()
        {
            TValue r;
            if (dict.TryGetValue(lookup, out r))
            {
                return r;
            }
            r = new TValue();
            dict[lookup] = r;
            return r;
        }

        // Helper for Dictionaries. Useful when TValue doesn't have a default ctor (such as with immutable objects like Tuples)
        public static TValue GetOrDefault<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey lookup, TValue defaultValue)
        {
            TValue r;
            if (dict.TryGetValue(lookup, out r))
            {
                return r;
            }
            r = defaultValue;
            dict[lookup] = r;
            return r;
        }

        public static void Assert(bool f)
        {
            Assert(f, String.Empty);
        }
        public static void Assert(bool f, string message)
        {
            if (!f)
            {
                throw new AssertException(message);
                //Debugger.Break();
            }
        }

        // Case insensitive string compare
        internal static bool Compare(string a, string b)
        {
            return string.Compare(a, b, true) == 0;
        }

        // All strings become upper case (for comparison)
        public static Dictionary<TKey, TValue> ToDict<TKey, TValue>(MutableDataTable table, string keyName, string valueName)
        {
            // $$$ Should this be on DataTable?
            int cKey = Utility.GetColumnIndexFromName(table.ColumnNames, keyName);
            int cValue = Utility.GetColumnIndexFromName(table.ColumnNames, valueName);
            return ToDict<TKey, TValue>(table, cKey, cValue);
        }

        public static Dictionary<TKey, TValue> ToDict<TKey, TValue>(MutableDataTable table)
        {
            // Assume first two
            return ToDict<TKey, TValue>(table, 0, 1);
        }

        // column ids to use for keys and values.
        public static Dictionary<TKey, TValue> ToDict<TKey, TValue>(MutableDataTable table, int cKey, int cVal)
        {
            Dictionary<TKey, TValue> d = new Dictionary<TKey, TValue>();
            for (int row = 0; row < table.NumRows; row++)
            {
                TKey k = Convert<TKey>(table.Columns[cKey].Values[row]);
                TValue v = Convert<TValue>(table.Columns[cVal].Values[row]);
                d[k] = v;
            }
            return d;
        }

        static T Convert<T>(string s)
        {
            return (T)System.Convert.ChangeType(s.ToUpperInvariant(), typeof(T));
        }

        internal static MutableDataTable ToMutable(DataTable table)
        {
            MutableDataTable dt = new MutableDataTable();

            // Take a pass through upfront so we know how large to allocate all the column arrays
            int numRows = table.Rows.Count();

            Column[] cs = Array.ConvertAll(table.ColumnNames.ToArray(), name => new Column(name, numRows));

            int rowIdx = 0;
            foreach (Row row in table.Rows)
            {
                var values = row.Values;
                for (int iColumn = 0; iColumn < values.Count; iColumn++)
                {
                    cs[iColumn].Values[rowIdx] = values[iColumn];
                }
                rowIdx++;
            }

            dt.Columns = cs;
            return dt;
        }



        // Dynamically Flatten. 
        // $$$ Need way to gaurantee that flatten order matches column names.
        public static MutableDataTable ToTableX<T>(IEnumerable<T> a, params string[] columnNames)
        {
            // $$$ How to infer column names?
            // Flatten doesn't have a definitive order.
            // If we had more smart collections, we could infer. 
            
            int count = a.Count();

            MutableDataTable d = new MutableDataTable();

            // Alloc columns
            Column[] cs = new Column[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                cs[i] = new Column(columnNames[i], count);
            }

            // Fill in rows
            int row = 0;
            foreach (T item in a)
            {
                string[] values = Flatten(item);
                Utility.Assert(values.Length == columnNames.Length);

                for (int i = 0; i < columnNames.Length; i++)
                {
                    cs[i].Values[row] = values[i];
                }

                row++;
            }

            d.Columns = cs;
            return d;            
        }

        // Given a type, get the column names. This is the corrollay to Flatten<T>, which given an instance
        // of the type, gets the values.
        // This must have parallel logic to Flatten<T>.
        internal static string[] InferColumnNames<T>()
        {
            Type t = typeof(T);
            if (t.IsPrimitive || t == typeof(string))
            {
                // No properties to lookat.
                return new string[1] {  "value" };
            }
            if (t.IsEnum || t == typeof(DateTime))
            {
                return new string[1] { t.Name };
            }
            
            return Array.ConvertAll(typeof(T).GetProperties(), prop => prop.Name);
        }

        // Exposed for testing
        internal static string[] Flatten<T>(T item)
        {
            List<string> vals = new List<string>();            
            FlattenWorker(item, vals);         
            return vals.ToArray();
        }
                
        private static void FlattenWorker(object item, List<string> vals)
        {
            if (item == null)
            {
                vals.Add(string.Empty);
                return;
            }
            Type t = item.GetType();

            // May need to flatten recursively
            if (t.IsPrimitive)
            {
                vals.Add(item.ToString());
                return;
            }

            if ((t == typeof(string)) || (t == typeof(DateTime)) || t.IsEnum || (t == typeof(DiscreteValue)))
            {
                vals.Add(item.ToString());
                return;
            }

            if (t.IsGenericType)
            {
                Type t2 = t.GetGenericTypeDefinition();

                if (t2 == typeof(KeyValuePair<,>))
                {
                    object key = GetMember(item, "Key");
                    FlattenWorker(key, vals);

                    object value = GetMember(item, "Value");
                    FlattenWorker(value, vals);

                    return;
                }
                
            }

            // It's a class, add public properties of the class.
            // $$$ If the class is polymorphic, then this could change for different instances.
            {
                PropertyInfo[] ps = t.GetProperties();
                foreach (var p in ps)
                {
                    FlattenWorker(GetMember(item, p.Name), vals);
                }
                return;
            }

            // If it's a tuple, 


            // If is a key-value pair?
            throw new NotImplementedException();
        }

        static object GetMember(object o, string memberName)
        {
            Type t = o.GetType();
            PropertyInfo p = t.GetProperty(memberName);
            return p.GetValue(o, null);
        }


        // $$$ Merge with the more dynamic ToTable.
        internal static MutableDataTable ToTable<T1, T2>(Tuple<T1, T2>[] a, string name1, string name2)
        {
            MutableDataTable d = new MutableDataTable();

            int count = a.Length;
            Column cKeys = new Column(name1, count);
            Column cVals = new Column(name2, count);

            d.Columns = new Column[] { cKeys, cVals };

            int i = 0;
            foreach (var kv in a)
            {
                cKeys.Values[i] = kv.Item1.ToString();
                cVals.Values[i] = kv.Item2.ToString();
                i++;
            }
            return d;
        }
                


        // Convert a 2d dict into a 2d data table.
        // TKey1 is rows, TKey1 is columns.
        // Data table column names are obtained from key values.
        // Column 0 is set of row values.
        internal static MutableDataTable ToTable<TKey1, TKey2, TValue>(Dictionary2d<TKey1, TKey2, TValue> dict)
        {
            // TKey1 is rows, TKey2 is values.
            MutableDataTable d = new MutableDataTable();

            var rows = dict.Key1;
            int count = rows.Count();

            // Set columns
            var columns = dict.Key2.ToArray();
            {
                Column[] cs = new Column[columns.Length + 1];
                cs[0] = new Column("row name", count);
                for (int ic = 0; ic < columns.Length; ic++)
                {
                    cs[ic + 1] = new Column(columns[ic].ToString(), count);
                }
                d.Columns = cs;
            }

            // Add rows
            int i = 0;
            foreach (var row in rows)
            {
                d.Columns[0].Values[i] = row.ToString();
                for (int ic = 0; ic < columns.Length; ic++)
                {
                    d.Columns[ic + 1].Values[i] = dict[row, columns[ic]].ToString();
                }
                i++;
            }

            return d;
        }

        internal static int GetColumnIndexFromName(IEnumerable<string> columnNames, string columnName)
        {
            int i = 0;
            foreach (string x in columnNames)
            {
                if (string.Compare(columnName, x, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    return i;
                }
                i++;
            }
            throw new InvalidOperationException("No column named '" + columnName + "'");
        }
    }    
}
