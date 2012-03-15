using System.Diagnostics;

namespace DataAccess {
    public class Column {
        public string Name; // name of this column
        public string[] Values; // value per row.

        public Column(string name, int numRows) {
            this.Name = name;
            this.Values = new string[numRows];
        }

        public override string ToString() {
            int count = 0;
            if (this.Values != null) {
                count = this.Values.Length;
            }
            return string.Format("{0} ({1} entries)", this.Name, count);
        }

        // Check if this column has any data in it.
        public bool CheckIsEmpty() {
            foreach (var v in this.Values) {
                if (!string.IsNullOrEmpty(v)) {
                    return false;
                }
            }
            return true;
        }
    }



}