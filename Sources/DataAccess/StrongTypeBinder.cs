using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq.Expressions;
 


namespace DataAccess
{
    // Binder for creating a strong type from a Row
    internal class StrongTypeBinder
    {
        // Map from type property names to column names.
        internal static Type[] GetTypes(Type target, DataTable table)
        {
            string[] columnNamesNormalized = table.ColumnNames.ToArray();
            Type[] types = new Type[columnNamesNormalized.Length];

            columnNamesNormalized = Array.ConvertAll(columnNamesNormalized, Normalize);

            PropertyInfo[] targetProperties = target.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            foreach (PropertyInfo p in targetProperties)
            {
                int index = LookupRowIndex(columnNamesNormalized, p.Name);
                if (index >= 0)
                {
                    types[index] = p.PropertyType;
                }
            }

            return types;
        }

        // Create a strongly-typed custom parse method for the object. 
        // This can frontload all type analysis and generate a dedicated method that avoids Reflection. 
        public static Func<Row, T> BuildMethod<T>(IEnumerable<string> columnNames, Dictionary<string, string> mappingDictionary = null)
        {
            ParameterExpression param = Expression.Parameter(typeof(Row), "row");

            Type target = typeof(T);

            string[] columnNamesNormalized = columnNames.ToArray();
            columnNamesNormalized = Array.ConvertAll(columnNamesNormalized, element => NormalizeWithMappings(element, mappingDictionary));

            PropertyInfo[] targetProperties = target.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            if ((targetProperties.Length == 0) && (columnNamesNormalized.Length == 1))
            {
                // If it's just a single column and we have no properties (such as for int, guid, double, etc)
                // Just parse row.Values[0]
                int index = 0;
                MethodInfo miLookup = ((Func<Row, int, string>)LookupExpression).Method;
                var lookupExpr = Expression.Call(miLookup, param, Expression.Constant(index)); // (row,int) --> string
                var parseResultExpr = GetParseExpression(target, lookupExpr); // string --> T
                                
                return Expression.Lambda<Func<Row, T>>(parseResultExpr, param).Compile();
            }

            // Produce a delegate like:
            // T Parse(Row row){ 
            //   newObj = new T();
            //   newObj.Prop1 = Parse(row.Values[i1]);  
            //   newObj.Prop2 = Parse(row.Values[12]);
            //   ...
            //   return newObj
            // }
            //
            // i1,i2, are computed at build time doing name matching. 
            // Parse() function is determined at build time based on property type. 
            List<Expression> statements = new List<Expression>();
            var newObj = Expression.Variable(target, "target");

            statements.Add(Expression.Assign(newObj, Expression.New(target)));
            foreach (PropertyInfo p in targetProperties)
            {
                if (p.CanWrite)
                {
                    int index = LookupRowIndex(columnNamesNormalized, p.Name);
                    if (index == -1)
                    {
                        // Ignore properties where no matching column, leave as default value.
                        // Except set strings to string.Empty instead of null.
                        if (p.PropertyType == typeof(string))
                        {
                            var setExpr = Expression.Call(newObj, p.GetSetMethod(), Expression.Constant(string.Empty));
                            statements.Add(setExpr);
                        }
                    }
                    else
                    {
                        MethodInfo miLookup = ((Func<Row, int, string>)LookupExpression).Method;
                        var lookupExpr = Expression.Call(miLookup, param, Expression.Constant(index));
                        var parseResultExpr = GetParseExpression(p.PropertyType, lookupExpr);
                        var setExpr = Expression.Call(newObj, p.GetSetMethod(), parseResultExpr);
                        statements.Add(setExpr);
                    }
                }
            }
            statements.Add(newObj); // return result

            Expression body = Expression.Block(new[] { newObj }, statements);

            Func<Row, T> lambda =
                Expression.Lambda<Func<Row, T>>(
                body, param).Compile();

            return lambda;
        }

        // Find the column index for the given name. 
        // This is done once when creating the delegate, so it can have heavier string pattern matching logic. 
        // This lets runtime just do an index lookup.
        // columnNames have already been normalized. 
        static int LookupRowIndex(string[] columnNamesNormalized, string columnNameLookup)
        {
            columnNameLookup = Normalize(columnNameLookup);
            int i = 0;
            foreach(string x in columnNamesNormalized)
            {
                if (string.Compare(x, columnNameLookup, ignoreCase: true) == 0)
                {
                    return i;
                }
                i++;
            }
            return -1;
        }

        // Normalize the column name using mapping Dictionary else Convert to upper case, alphanumeric.
        private static string NormalizeWithMappings(string name, Dictionary<string, string> mappingDictionary = null)
        {
            mappingDictionary = mappingDictionary != null
                ? mappingDictionary.ToDictionary(kp => kp.Value, kp => kp.Key)
                : new Dictionary<string, string>();

            return mappingDictionary.ContainsKey(name) ? Normalize(mappingDictionary[name]) : Normalize(name);
        }

        // Normalize the column name. Convert to upper case, alphanumeric.
        static string Normalize(string name)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append('_'); // avoids first char issues.
            foreach (var ch in name)
            {
                if (ch >= 'a' && ch <= 'z')
                {
                    sb.Append((char) (ch - 'a' + 'A'));
                }
                else if (ch >= 'A' && ch <= 'Z')
                {
                    sb.Append(ch);
                }
                else if (ch >= '0' && ch <= '9')
                {
                    sb.Append(ch);
                }
                // ignore all other chars
            }
            return sb.ToString();
        }

        // runtime helper to find the expression. 
        // index should be valid. Delegate builder already had a chance to analyze column names at compile time
        // and so avoid this method if the column doesn't exist.
        static string LookupExpression(Row row, int index)
        {
            return row.Values[index];
        }

        // Get an Expression tree which will parse the string (provided by value), and return a result of type Type, eg:
        //   Func: string --> Type
        // This can do static analysis on type to return an efficient parse function.
        // This avoids a runtime search on type.         
        static Expression GetParseExpression(Type type, Expression value)
        {
            // Input parameter is a string, which we'll parse. 
            Debug.Assert(value.Type == typeof(string));

            // If it's a string, just return directly.
            if (type == typeof(string))
            {
                return value;
            }

            if (type == typeof(double))
            {
                MethodInfo parseDoubleMethod = ((Func<string, double>)ToDouble).Method;
                return Expression.Call(parseDoubleMethod, value);
            }

            // If it has a TryParse function, call that. That's much faster than a Type converter
            MethodInfo tryParseMethod = type.GetMethod("TryParse", new[] { typeof(string), type.MakeByRefType() });
            if (tryParseMethod != null)
            {
                // can't pass a property as an out parameter, so we need a temporary local.
                // compile as:
                // {   T temp;
                //     TryParse(value, out temp);
                //     return temp 
                // }
                var temp = Expression.Variable(type);
                return Expression.Block(new[] { temp }, // define Local
                    Expression.Call(tryParseMethod, value, temp),
                    temp); // return temp
            }

            {
                // Type converter lookup is slow and can be hoisted in the closure and done statically. 
                var converter = GetConverter(type);
                var converterExpr = Expression.Constant(converter); // hoisted

                // compile:
                //    { return (T) converter.ConvertFrom(value); }
                var convertMethod = ((Func<object, object>)converter.ConvertFrom).Method;
                var exprCall = Expression.Call(converterExpr, convertMethod, value);
                return Expression.Convert(exprCall, type);
            }
        }

        // BCL implementation may get wrong converters
        // It appears to use Type.GetType() to find a converter, and so has trouble looking up converters from different loader contexts.
        static TypeConverter GetConverter(Type type)
        {
            // $$$ There has got to be a better way than this to make TypeConverters work.
            foreach (TypeConverterAttribute attr in type.GetCustomAttributes(typeof(TypeConverterAttribute), false))
            {
                string assemblyQualifiedName = attr.ConverterTypeName;
                if (!string.IsNullOrWhiteSpace(assemblyQualifiedName))
                {
                    // Type.GetType() may fail due to loader context issues.
                    string assemblyName = type.Assembly.FullName;

                    if (assemblyQualifiedName.EndsWith(assemblyName))
                    {
                        int i = assemblyQualifiedName.IndexOf(',');
                        if (i > 0)
                        {
                            string typename = assemblyQualifiedName.Substring(0, i);

                            var a = type.Assembly;
                            var t2 = a.GetType(typename); // lookup type name relative to the 
                            if (t2 != null)
                            {
                                var instance = Activator.CreateInstance(t2);
                                return (TypeConverter)instance;
                            }
                        }
                    }
                }
            }

            return TypeDescriptor.GetConverter(type);
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

            return double.NaN;
        }
    }
}
