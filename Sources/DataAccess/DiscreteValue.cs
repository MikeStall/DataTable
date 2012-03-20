using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataAccess
{

    // Like a enum, but structural equivalence. 
    // Also deal with case-insensitive strings
    // provides referential equivalence 
    // Only supports equality comparison. No ordinal comparison. Not continuous. 
    internal class DiscreteValueKind
    {
        Dictionary<string, int> m_map = new Dictionary<string, int>();
        List<DiscreteValue> m_list = new List<DiscreteValue>();
        List<string> m_strings = new List<string>();

        string m_label;

        public void AddValues(IEnumerable<string> s)
        {
            foreach (var value in s)
            {
                Add(value);
            }
        }
        private DiscreteValue Add(string value)
        {
            // No dups
            int id = m_list.Count;
            var d = new DiscreteValue(this, id);
            m_list.Add(d);

            string x = value.ToUpperInvariant();
            m_strings.Add(x);

            m_map.Add(x, id);
            return d;
        }

        public DiscreteValueKind()
        {
        }

        // Create discrete value for the given column
        public DiscreteValueKind(Column c)
        {
            m_label = c.Name;
            AddValues(c.Values);
        }

        public DiscreteValue LookupOrAdd(string value)
        {
            string x = value.ToUpperInvariant();
            int id;
            if (m_map.TryGetValue(x, out id))
            {
                return m_list[id];
            }
            // Not in list.
            return Add(value);

        }

        public DiscreteValue Lookup(string value)
        {
            string x = value.ToUpperInvariant();
            int id = m_map[x]; // throw if missing
            return m_list[id];
        }
        internal string Lookup(int index)
        {
            return m_strings[index];
        }
    }

    // Values have referential equivalence.
    // authority it the _kind
    internal class DiscreteValue
    {
        DiscreteValueKind _kind;
        int _index;

        internal DiscreteValue(DiscreteValueKind kind, int value)
        {
            _kind = kind;
            _index = value;
        }

        public override string ToString()
        {
            return _kind.Lookup(_index);
        }
    }

}
