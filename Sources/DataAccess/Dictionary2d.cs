using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataAccess
{
    // 2d Dictionairy
    // Sparse implementation. 
    // Implementation is not intended for large sizes. Rather, we expec this to be used
    // for summarizing information.
    public class Dictionary2d<TKey1, TKey2, TValue>
    {
        Dictionary<Tuple<TKey1, TKey2>, TValue> _dict = new Dictionary<Tuple<TKey1, TKey2>, TValue>();

        public int Count
        {
            get { return _dict.Count; }
        }

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

        // Enumerate all keys

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
