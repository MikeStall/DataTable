using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataAccess
{

    // This is the heart of the parsing logic. All other parsing operations should call into this. 
    // This handles all the wacky corner cases like newlines in quoted values.
    // This is internal. Use the Builder functions to access them.
    // A CSV description is here:
    // http://www.creativyst.com/Doc/Articles/CSV/CSV01.htm 
    internal class Reader
    {
        SplitState _currentState;

        List<string> _parts = new List<string>();
        StringBuilder _sb = new StringBuilder();
        bool _captureValue;



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
            Reader r = new Reader();
            r.StartRow();
            foreach (var ch in input)
            {
                r.ProcessSingleChar(ch, separator, trim);
            }
            return r.DoneRow(trim);
        }

        public void StartRow()
        {
            rowCount++;
            _currentState = SplitState.Start;
            _parts.Clear();
            _sb.Length = 0;

            _captureValue = IncludeColumn(_parts.Count);
        }

        private bool IncludeColumn(int columnIdx)
        {
            // $$$
            return true;
        }

        public char chSeparator; // Guess the separator from the first row
        public int rowCount = -1; // before start of rist row
        public bool errorMode; // hit an error, just jump to the next newline
        public int ignore; // really bad failures. 

        const char EOFChar = unchecked((char) -1);
        public string[] ProcessEndOfFile(bool trim)
        {
            return ProcessChar(EOFChar, trim);
        }

        // Process the character. If we're at the end of a row, return the values. 
        // If we're int he middle of a row, return null.
        // accept -1 as a EOF terminator to cooperate with STream.ReadByte(). 
        public string[] ProcessChar(char ch, bool trim)
        {
            if (ch == EOFChar)
            {
                if (this.HasContent())
                {
                    ch = '\n';
                }
                else
                {
                    return null;
                }
            }
            if (ch == '\r')
            {
                if (!ShouldNewlineBeContent())
                {
                    return null;
                }
            }
            if (ch == '\n')
            {
                if (!errorMode)
                {
                    if (!ShouldNewlineBeContent())
                    {
                        var values = this.DoneRow(trim);
                        StartRow();
                        return values;
                    }
                }
                else
                {
                    errorMode = false;
                    return null;
                }
            }

            // Guess separator from contents.
            if (chSeparator == 0)
            {
                if (ch == '\t')
                {
                    chSeparator = '\t';
                }
                else if (ch == ',')
                {
                    chSeparator = ',';
                }
                else if (ch == ';')
                {
                    chSeparator = ';';
                }
                else if (ch == '|')
                {
                    chSeparator = '|';
                }
            }

            try
            {
                if (!errorMode)
                {
                    ProcessSingleChar(ch, chSeparator, trim);
                }
            }
            catch (AssertException e)
            {
                // Something really corrupt about this row. Ignore it. 
                ignore++;

                Console.WriteLine("$$$ Error at row: {0} {1}", rowCount, e.Message);
                errorMode = true;
                StartRow();
            }
            return null;
        }

        public void ProcessSingleChar(char ch, char separator, bool trim)
        {
            switch (_currentState)
            {
                case SplitState.Start:
                    if (ch == '"')
                    {
                        _currentState = SplitState.StartQuote;
                    }
                    else if (ch == separator)
                    {
                        _currentState = SplitState.StartSeparator;
                    }
                    else if (ch == ' ')
                    {
                        _currentState = SplitState.PotentialStartSpace;
                    }
                    else
                    {
                        _currentState = SplitState.Word;
                    }
                    break;
                case SplitState.StartQuote:
                    if (ch == '"')
                    {
                        _currentState = SplitState.PotentialEndQuote;
                    }
                    else
                    {
                        _currentState = SplitState.EscapedWord;
                    }
                    break;
                case SplitState.StartSeparator:
                    if (ch == '"')
                    {
                        _currentState = SplitState.StartQuote;
                    }
                    else if (ch == separator)
                    {
                        break;
                    }
                    else if (ch == ' ')
                    {
                        _currentState = SplitState.PotentialStartSpace;
                    }
                    else
                    {
                        _currentState = SplitState.Word;
                    }
                    break;
                case SplitState.PotentialStartSpace:
                    if (ch == '"')
                    {
                        _currentState = SplitState.StartQuote;
                        _sb.Length = 0;
                    }
                    else if (ch == separator)
                    {
                        _currentState = SplitState.StartSeparator;
                    }
                    else if (ch != ' ')
                    {
                        _currentState = SplitState.Word;
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
                        _currentState = SplitState.StartSeparator;
                    }
                    break;
                case SplitState.EscapedWord:
                    if (ch == '"')
                    {
                        _currentState = SplitState.PotentialEndQuote;
                    }
                    break;
                case SplitState.PotentialEndQuote:
                    if (ch == '"')
                    {
                        _currentState = SplitState.EscapedWord;
                    }
                    else if (ch == separator)
                    {
                        _currentState = SplitState.StartSeparator;
                    }
                    else if (ch == ' ')
                    {
                        _currentState = SplitState.PotentialEndSpace;
                    }
                    else
                    {
                        _currentState = SplitState.UnescapedQuote;
                    }
                    break;
                case SplitState.PotentialEndSpace:
                    if (ch == separator)
                    {
                        _currentState = SplitState.StartSeparator;
                    }
                    else if (ch != ' ')
                    {
                        _currentState = SplitState.UnescapedQuote;
                    }
                    // Anything else is a case like: "abc" d
                    // does that parse as 'abc d'?  Is it an error?
                    break;
                default:
                    break;
            }

            switch (_currentState)
            {
                case SplitState.StartSeparator:
                    {
                        PushValue(trim);
                    }
                    break;

                case SplitState.PotentialStartSpace:
                case SplitState.Word:
                case SplitState.EscapedWord:
                case SplitState.PotentialEndSpace:
                    if (_captureValue)
                    {
                        _sb.Append(ch);
                    }
                    break;

                case SplitState.UnescapedQuote:
                    throw new AssertException("unescaped double quote");

                case SplitState.MissingEndQuote:
                    throw new AssertException("missing closing quote");
            }
        }

        private void PushValue(bool trim)
        {
            string x;
            if (_captureValue)
            {

                x = _sb.ToString();
                _sb.Length = 0;
                if (trim) { x = x.Trim(); }
            }
            else
            {
                x = string.Empty;
            }
            _parts.Add(x);

            _captureValue = this.IncludeColumn(_parts.Count);
        }

        // Are we in the middle of a word? IE, should newlines count as part of the value?
        public bool ShouldNewlineBeContent()
        {
            return _currentState == SplitState.EscapedWord;
        }
        public string[] DoneRow(bool trim)
        {
            // add leftovers
            PushValue(trim);
           
            return _parts.ToArray();
        }

        public bool HasContent()
        {
            if (_sb == null || _sb.Length == 0)
            {
                return false;
            }
            return !string.IsNullOrWhiteSpace(_sb.ToString());
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

        private static IList<string> ReadAllLines(string filename)
        {
            using (TextReader tr = new StreamReader(filename))
            {
                return ReadAllLines(tr);
            }
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

                // Some CSVs have blank links, like "\r\r\n" as a line terminator. 
                // Ignore the extra blank,.
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
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

            if (header.Contains("|"))
            {
                return '|';
            }
            
            // Fallback is always comma. This implies a single column. 
            return ',';
            
        }

        // Read in a Ascii file that uses the given separate characters.
        // Like CSV. 
        // Supports quotes to escape commas
        public static MutableDataTable Read(string filename, char separator = '\0', bool fAllowMismatch = false, string[] defaultColumns = null)
        {
            var lines = ReadAllLines(filename);
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

                while (lineEnumerator.MoveNext())
                {
                    string line = lineEnumerator.Current;

                    row++;

                    string[] parts = split(line, separator);

                    if (parts.Length < numColumns)
                    {
                        // Deal with possible extra commas at the end. 
                        // Excel handles this. 
                        for (int c = 0; c < parts.Length; c++)
                        {
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

                    if (!fAllowMismatch)
                    {
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
                    for (int c = 0; c < numColumns; c++)
                    {
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