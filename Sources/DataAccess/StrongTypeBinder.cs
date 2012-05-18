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
        // Create a strongly-typed custom parse method for the object. 
        // This can frontload all type analysis and generate a dedicated method that avoids Reflection. 
        public static Func<Dictionary<string, string>, T> BuildMethod<T>()
        {
            ParameterExpression param = Expression.Parameter(typeof(Dictionary<string, string>), "row");

            Type target = typeof(T);

            List<Expression> statements = new List<Expression>();
            var newObj = Expression.Variable(target, "target");

            statements.Add(Expression.Assign(newObj, Expression.New(target)));
            foreach (PropertyInfo p in target.GetProperties())
            {
                if (p.CanWrite)
                {
                    MethodInfo miLookup = ((Func<Dictionary<string, string>, string, string>)LookupExpression).Method;
                    var lookupExpr = Expression.Call(miLookup, param, Expression.Constant(p.Name));
                    var parseResultExpr = GetParseExpression(p.PropertyType, lookupExpr);
                    var setExpr = Expression.Call(newObj, p.GetSetMethod(), parseResultExpr);
                    statements.Add(setExpr);
                }
            }
            statements.Add(newObj); // return result

            Expression body = Expression.Block(new[] { newObj }, statements);

            Func<Dictionary<string, string>, T> lambda =
                Expression.Lambda<Func<Dictionary<string, string>, T>>(
                body, param).Compile();

            return lambda;
        }

        // runtime helper to find the expression. 
        static string LookupExpression(Dictionary<string, string> row, string constant)
        {
            return row[constant];
        }

        // Get an Expression tree which will parse the string (provided by value), and return a result of type Type. 
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
                var converter = TypeDescriptor.GetConverter(type);
                var converterExpr = Expression.Constant(converter); // hoisted

                // compile:
                //    { return (T) converter.ConvertFrom(value); }
                var convertMethod = ((Func<object, object>)converter.ConvertFrom).Method;
                var exprCall = Expression.Call(converterExpr, convertMethod, value);
                return Expression.Convert(exprCall, type);
            }
        }
    }
}
