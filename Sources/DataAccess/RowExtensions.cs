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
        /// It's much faster to call DataTable.RowsAs[T] instead, because that will reuse parsing logic across rows.
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
    }
}
