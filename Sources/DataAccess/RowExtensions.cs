using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace DataAccess
{
    /// <summary>
    /// Extension methods for Row object
    /// </summary>
    public static class RowExtension
    {

        /// <summary>
        /// Makes a best effort to parse the row into a strongly typed object. 
        /// This does a case-insensitive match of the Target object's property names against the table's column names.
        /// Parse errors are ignored and may produce invalid results for the corresponding cell.
        /// </summary>
        /// <typeparam name="T">Target object type to parse.</typeparam>
        /// <param name="row">incoming row to be parsed</param>
        /// <returns>an object representing the row</returns>
        public static T As<T>(this Row row) where T : new()
        {
            var parser = StrongTypeBinder.BuildMethod<T>(row.ColumnNames);
            var c = parser(row);
            return c;
        }

        /// <summary>
        /// Makes a best effort to parse the row into a strongly typed object. 
        /// This does a case-insensitive match of the Target object's property names against the table's column names.
        /// Parse errors are ignored and may produce invalid results for the corresponding cell.
        /// </summary>
        /// <typeparam name="T">Target object type to parse.</typeparam>
        /// <param name="row">incoming row to be parsed</param>
        /// <returns>an object representing the row</returns>
        public static T As2<T>(this Row row) where T : new()
        {
            var type = typeof(T);
            // check if the Type is a primitive. Maybe we're just extracting a single row.
            
            if (row.Values.Count == 1)
            {
                object result = ConvertString(type, row.Values[0]);
                return (T) result;
            }

            T target = new T();

            foreach (PropertyInfo p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!p.CanWrite)
                {
                    continue;
                }
                string val = row.GetValueOrEmpty(p.Name);
                object result = ConvertString(p.PropertyType, val);

                p.SetValue(target, result, null);
            }
            return target;
        }

        // Convert string to a target type.
        // Return result is of type target. 
        private static object ConvertString(Type target, string value)
        {
            if (target == typeof(string))
            {
                return value;
            }
            
            // Special handling of % character with double.
            if (target == typeof(double))
            {
                return ToDouble(value);
            }            

            var conv = System.ComponentModel.TypeDescriptor.GetConverter(target);
            if (conv != null)
            {
                try
                {
                    return conv.ConvertFrom(value);
                }
                catch
                {
                    // If failed to convert, return null.
                    return null;
                }
            }

            // Enums
            if (target.IsEnum)
            {
                return Enum.Parse(target, value, ignoreCase: true);
            }

            return null;
        }

        // Parse a double, handle percents.
        // Return NaN on failure.
        private static double ToDouble(string s)
        {
            double result;
            if (double.TryParse(s, out result))
            {
                return result;
            }

            // Handle percents. 100% --> 1
            if (s.EndsWith("%"))
            {
                string s2 = s.Substring(0, s.Length - 1);
                if (double.TryParse(s2, out result))
                {
                    return result / 100.0;
                }
            }

            return double.NaN ;
        }
    }
}
