using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataAccess
{

    // General reader utilities. 
    // This is internal. Use the Builder functions to access them.
    // A CSV description is here:
    // http://www.creativyst.com/Doc/Articles/CSV/CSV01.htm 
    internal static class Reader
    {

        static string Intern(string value) {
            //return string.Intern(value);
            return value;
        }
        // split the string, and then trim each part
        // items can be quoted. 
        // A, B, C
        // A, "B1, B2", C        
        // - trim whitespace 
        public static string[] split(string input, char separator) {
            return split(input, separator, true);
        }
        
        private enum SplitState
        {
            Start = 0,
            StartQuote,
            StartSeparator,
            PotentialStartSpace,
            Word,
            EscapedWord,
            PotentialEndQuote,
            PotentialEndSpace,
            UnescapedQuote,
            MissingEndQuote
        }
        
        public static string[] split(string input, char separator, bool trim) 
        {
            SplitState currentState = SplitState.Start;
            List<string> parts = new List<string>();

            StringBuilder sb = new StringBuilder();
            foreach (char ch in input)
            {
                switch (currentState)
                {
                    case SplitState.Start:
                        if (ch == '"')
                        {
                            currentState = SplitState.StartQuote;
                        }
                        else if (ch == separator)
                        {
                            currentState = SplitState.StartSeparator;
                        }
                        else if (ch == ' ')
                        {
                            currentState = SplitState.PotentialStartSpace;
                        }
                        else
                        {
                            currentState = SplitState.Word;
                        }
                        break;
                    case SplitState.StartQuote:
                        if (ch == '"')
                        {
                            currentState = SplitState.PotentialEndQuote;
                        }
                        else
                        {
                            currentState = SplitState.EscapedWord;
                        }
                        break;
                    case SplitState.StartSeparator:
                        if (ch == '"')
                        {
                            currentState = SplitState.StartQuote;
                        }
                        else if (ch == separator)
                        {
                            break;
                        }
                        else if (ch == ' ')
                        {
                            currentState = SplitState.PotentialStartSpace;
                        }
                        else
                        {
                            currentState = SplitState.Word;
                        }
                        break;
                    case SplitState.PotentialStartSpace:
                        if (ch == '"')
                        {
                            currentState = SplitState.StartQuote;
                            sb.Length = 0;
                        }
                        else if (ch == separator)
                        {
                            currentState = SplitState.StartSeparator;
                        }
                        else if (ch != ' ')
                        {
                            currentState = SplitState.Word;
                        }
                        break;
                    case SplitState.Word:
                        // Allow quotes in the middle of a word.
                        // a, b "b", c
                        if (ch == '"')
                        {
                            //currentState = SplitState.UnescapedQuote;
                        }
                        else if (ch == separator)
                        {
                            currentState = SplitState.StartSeparator;
                        }
                        break;
                    case SplitState.EscapedWord:
                        if (ch == '"')
                        {
                            currentState = SplitState.PotentialEndQuote;
                        }
                        break;
                    case SplitState.PotentialEndQuote:
                        if (ch == '"')
                        {
                            currentState = SplitState.EscapedWord;
                        }
                        else if (ch == separator)
                        {
                            currentState = SplitState.StartSeparator;
                        }
                        else if (ch == ' ')
                        {
                            currentState = SplitState.PotentialEndSpace;
                        }
                        else
                        {
                            currentState = SplitState.UnescapedQuote;
                        }
                        break;
                    case SplitState.PotentialEndSpace:
                        if (ch == separator)
                        {
                            currentState = SplitState.StartSeparator;
                        }
                        else if (ch != ' ')
                        {
                            currentState = SplitState.UnescapedQuote;
                        }
                        break;
                    default:
                        break;
                }

                if (currentState == SplitState.StartSeparator)
                {
                    string x = sb.ToString();
                    if (trim) { x = x.Trim(); }
                    parts.Add(Intern(x));
                    sb.Length = 0;
                }

                if ((currentState == SplitState.PotentialStartSpace) ||
                    (currentState == SplitState.Word) ||
                    (currentState == SplitState.EscapedWord) ||
                    (currentState == SplitState.PotentialEndSpace))
                {
                    sb.Append(ch);
                }

                Utility.Assert(currentState != SplitState.UnescapedQuote, "unescaped double quote");
                Utility.Assert(currentState != SplitState.MissingEndQuote, "missing closing quote");
            }

            // add leftovers
            string lastItem = sb.ToString();
            if (trim) {lastItem = lastItem.Trim();}
            parts.Add(Intern(lastItem));

            return parts.ToArray();
        }

        public static MutableDataTable ReadTab(string filename) {
            return Read(filename, '\t');
        }
        public static MutableDataTable ReadCSV(string filename) {
            return Read(filename, ',');
        }

         
        public static MutableDataTable ReadCSV(TextReader stream)
        {
            IList<string> lines = ReadAllLines(stream);
            return ReadArray(lines, ',', false);            
        }

        private static IList<string> ReadAllLines(TextReader stream)
        {
            List<string> lines = new List<string>();
            while (true)
            {
                string line = stream.ReadLine();
                if (line == null)
                {
                    return lines;
                }
                lines.Add(line);
            }   
        }

        private static IList<string> ReadAllLines(string text, string newLine = "\r\n")
        {
            var lines = text.Split(new [] {newLine}, StringSplitOptions.RemoveEmptyEntries).ToList();
            return lines;
        }

        public static MutableDataTable Read(TextReader stream, char delimiter = '\0', string[] defaultColumns = null)
        {
            IList<string> lines = ReadAllLines(stream);
            return ReadArray(lines, delimiter, false, defaultColumns);
        }

        public static MutableDataTable ReadString(string text, string newLine = "\r\n", char delimiter = '\0', string[] defaultColumns = null)
        {
            IList<string> lines = ReadAllLines(text, newLine);
            return ReadArray(lines, delimiter, false, defaultColumns);
        }

        public static char GuessSeparateFromHeaderRow(string header)
        {
            if (header.Contains("\t"))
            {
                return '\t';
            }

            if (header.Contains(","))
            {
                return ',';
            }

            if (header.Contains(";"))
            {
                return ';';
            }
            
            // Fallback is always comma. This implies a single column. 
            return ',';
            
        }

        // Read in a Ascii file that uses the given separate characters.
        // Like CSV. 
        // Supports quotes to escape commas
        public static MutableDataTable Read(string filename, char separator = '\0', bool fAllowMismatch = false, string[] defaultColumns = null)
        {
            var lines = File.ReadAllLines(filename);
            MutableDataTable dt = ReadArray(lines, separator, fAllowMismatch, defaultColumns);
            dt.Name = filename;
            return dt;
        }


        private static MutableDataTable ReadArray(IList<string> lines, char separator, bool fAllowMismatch = false, string[] defaultColumns = null)
        {
            if (separator == '\0')
            {
                separator = GuessSeparateFromHeaderRow(lines[0]);
            }

            int numRows = lines.Count - (defaultColumns != null ? 0 : 1);
            // First row is a header only if we dont pass defaultColumns

            // if defaultColumns is not null then we use them as columns
            string[] names = defaultColumns ?? split(lines[0], separator);

            int numColumns = names.Length;

            var columns = new Column[numColumns];
            for (int i = 0; i < numColumns; i++) {
                columns[i] = new Column(names[i], numRows);
            }

            // Parse each row into data set
            using (var lineEnumerator = lines.GetEnumerator())
            {
                if (defaultColumns == null)
                {
                    lineEnumerator.MoveNext(); // in this case we have columns at first index
                }
                var row = -1;

                while(lineEnumerator.MoveNext())
                {
                    string line = lineEnumerator.Current;

                    row++;

                string[] parts = split(line, separator);

                if (parts.Length < numColumns) {
                    // Deal with possible extra commas at the end. 
                    // Excel handles this. 
                    for (int c = 0; c < parts.Length; c++) {
                        columns[c].Values[row] = parts[c];
                    }

                        if (fAllowMismatch)
                        {
                            for (int c = parts.Length; c < numColumns; c++)
                            {
                        columns[c].Values[row] = String.Empty;
                    }
                    continue;
                }

                }

                if (!fAllowMismatch) {
                    // If mismatch allowed, then treat this row as garbage rather
                    // than throw an exception
                        Utility.Assert(
                            parts.Length == names.Length,
                            String.Format(
                                "Allow Mismatch is False. Line has incorrect number of parts. Line Number:{0}; Expected:{1}; Actual:{2}",
                                row + 1,
                                names.Length,
                                parts.Length));
                }
                for (int c = 0; c < numColumns; c++) {
                    columns[c].Values[row] = parts[c];
                }
            }
            }

            MutableDataTable data = new MutableDataTable();
            data.Columns = columns;


            return data;
        }
    }
}