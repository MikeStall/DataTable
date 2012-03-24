using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics.CodeAnalysis;

namespace DataAccess
{    
    /// <summary>
    /// 2d Dictionary, useful for spare storage. 
    /// Implementation is not intended for large sizes. Rather, we expect this to be used for summarizing information.
    /// </summary>
    /// <typeparam name="TKey1">type of first key</typeparam>
    /// <typeparam name="TKey2">type of second key</typeparam>
    /// <typeparam name="TValue">type of value</typeparam>
    [SuppressMessage("Microsoft.Design", "CA1005:AvoidExcessiveParametersOnGenericTypes", Justification = "by design")]
    public class Dictionary2d<TKey1, TKey2, TValue>
    {
        private Dictionary<Tuple<TKey1, TKey2>, TValue> _dict = new Dictionary<Tuple<TKey1, TKey2>, TValue>();

        /// <summary>
        /// Count of total entries in the collection
        /// </summary>
        public int Count
        {
            get { return _dict.Count; }
        }

        /// <summary>
        /// lookup a value. Returns a default value if not found.
        /// </summary>
        /// <param name="k1">first key </param>
        /// <param name="k2">second key</param>
        /// <returns>value stored at the given key pair.</returns>
        [SuppressMessage("Microsoft.Design", "CA1023:IndexersShouldNotBeMultidimensional")]
        public TValue this[TKey1 k1, TKey2 k2]
        {
            get
            {
                TValue val;
                _dict.TryGetValue(new Tuple<TKey1, TKey2>(k1, k2), out val);
                return val;
            }
            set
            {
                _dict[new Tuple<TKey1, TKey2>(k1, k2)] = value;
            }
        }

        /// <summary>
        /// Sorted enumeration of the first keyset.
        /// </summary>
        public IEnumerable<TKey1> Key1
        {
            get
            {
                HashSet<TKey1> keys = new HashSet<TKey1>();
                foreach (var k in _dict.Keys)
                {
                    keys.Add(k.Item1);
                }
                TKey1[] a = keys.ToArray();
                Array.Sort(a);
                return a;
            }
        }

        /// <summary>
        /// Sorted enumeration of the second keyset.
        /// </summary>
        public IEnumerable<TKey2> Key2
        {
            get
            {
                HashSet<TKey2> keys = new HashSet<TKey2>();
                foreach (var k in _dict.Keys)
                {
                    keys.Add(k.Item2);
                }
                TKey2[] a = keys.ToArray();
                Array.Sort(a);
                return a;
            }
        }
    }
}
