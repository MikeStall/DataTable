using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Services.Client;
using System.Xml.Linq;
using System.Xml;
using System.Data.Services.Common;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using System.Text.RegularExpressions;

namespace DataAccess
{
    // Bridge between a DataTable Row and a AzureTable TableServiceEntity
    [DataServiceKey("PartitionKey", "RowKey")]
    internal class GenericWriterEntity : TableServiceEntity
    {
        // Use a custom write hook to convert from a Row to EDM properties.
        public Row _source;        
    }

    internal class GenericTableWriter
    {
        private string[] _edmTypeNames;
        private string[] _columnNames;

        private static bool Compare(string a, string b)
        {
            return string.Compare(a, b, StringComparison.OrdinalIgnoreCase) == 0;
        }

        private static bool IsSpecialColumnName(string columnName)
        { 
            // Case-insensitive compare
            return Compare(columnName, "PartitionKey") || Compare(columnName, "RowKey") || Compare(columnName, "Timestamp");
        }

        // $$$ Should be some common helper. This is protected on Row; but should be on table. 
        private static int GetColumnIndex(string columnName, string[] columnNames)
        {
            for (int i = 0; i < columnNames.Length; i++)
            {
                if (Compare(columnNames[i], columnName))
                {
                    return i;
                }
            }
            return -1;
        }

        // Azure table names are very restrictive, so sanity check upfront to give a useful error.
        // http://msdn.microsoft.com/en-us/library/windowsazure/dd179338.aspx
        private static void ValidateAzureTableName(string tableName)
        {
            if (!Regex.IsMatch(tableName, "^[A-Za-z][A-Za-z0-9]{2,62}$"))
            {
                throw new InvalidOperationException(string.Format("{0} is not a valid name for an azure table", tableName));
            }
        }

        // Get a function that will determine the partition row key
        private static Func<int, Row, PartitionRowKey> GetPartitionRowKeyFunc(string[] columnNames)
        { 
            // If incoming table has columns named "PartitionKey" and "RowKey", then use those. 
            int iPartitionKey = GetColumnIndex("PartitionKey", columnNames);
            int iRowKey = GetColumnIndex("RowKey", columnNames);
            if (iPartitionKey >= 0 && iRowKey  >= 0)
            {
                // Both row and partition key
                return (rowIndex, row) => new PartitionRowKey(row.Values[iPartitionKey], row.Values[iRowKey]);
            }
            else if ((iPartitionKey < 0) && (iRowKey >= 0))
            {
                // Only row Key
                return (rowIndex, row) => new PartitionRowKey("1", row.Values[iRowKey]);
            }
            else if ((iPartitionKey >= 0) && (iRowKey < 0))
            {
                // Only a partition key
                return (rowIndex, row) => new PartitionRowKey(row.Values[iPartitionKey], rowIndex);
            }
            else
            {                    
                // format rowkey so that when sorted alpanumerically, it's still ascending
                return (rowIndex, row) => new PartitionRowKey("1", rowIndex);
            }            
        }

        // Write a DataTable to an AzureTable.
        // DataTable's Rows are an unstructured property bag.
        // columnTypes - type of the column, or null if column should be skipped. Length of columnTypes should be the same as number of columns.
        public static void SaveToAzureTable(DataTable table, CloudStorageAccount account, string tableName, Type[] columnTypes, Func<int, Row, PartitionRowKey> funcComputeKeys)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            if (account == null)
            {
                throw new ArgumentNullException("account");
            }
            if (columnTypes == null)
            {
                throw new ArgumentNullException("columnTypes");
            }
            if (tableName == null)
            {
                throw new ArgumentNullException("tableName");
            }
            ValidateAzureTableName(tableName);

            // Azure tables have "special" columns. 
            // We can skip these by settings columnType[i] to null, which means don't write that column
            string[] columnNames = table.ColumnNames.ToArray();
            if (columnNames.Length != columnTypes.Length)
            {
                throw new ArgumentException(string.Format("columnTypes should have {0} elements", columnNames.Length), "columnTypes");
            }

            columnTypes = columnTypes.ToArray(); // create a copy for mutation. 
            for (int i = 0; i < columnNames.Length; i++)
            {
                if (IsSpecialColumnName(columnNames[i]))
                {
                    columnTypes[i] = null;
                }
            }

            if (funcComputeKeys == null)
            {
                funcComputeKeys = GetPartitionRowKeyFunc(columnNames);
            }

            // Validate columnTypes 
            string [] edmTypeNames = Array.ConvertAll(columnTypes, 
                 columnType => {
                     if (columnType == null)
                     {
                         return null;
                     }
                     string edmTypeName;
                     _edmNameMapping.TryGetValue(columnType, out edmTypeName);
                     if (edmTypeName == null)
                     {
                         // Unsupported type!
                         throw new InvalidOperationException(string.Format("Type '{0}' is not a supported type on azure tables", columnType.FullName));
                     }
                     return edmTypeName;
                 });


            CloudTableClient tableClient = account.CreateCloudTableClient();

            tableClient.DeleteTableIfExist(tableName);
            tableClient.CreateTableIfNotExist(tableName);
            
            
            GenericTableWriter w = new GenericTableWriter 
            {
                _edmTypeNames = edmTypeNames,
                _columnNames = table.ColumnNames.ToArray()
            };
            
            // Batch rows for performance, 
            // but all rows in the batch must have the same partition key
            TableServiceContext ctx = null;
            string lastPartitionKey = null;

            HashSet<PartitionRowKey> dups = new HashSet<PartitionRowKey>();

            int rowCounter = 0;
            int batchSize = 0;
            foreach (Row row in table.Rows)
            {
                GenericWriterEntity entity = new GenericWriterEntity { _source = row };
                // Compute row and partition keys too. 
                var partRow = funcComputeKeys(rowCounter, row);
                entity.PartitionKey = partRow.PartitionKey;
                entity.RowKey = partRow.RowKey;
                rowCounter++;

                // but all rows in the batch must have the same partition key
                if ((ctx != null) && (lastPartitionKey != null) && (lastPartitionKey != entity.PartitionKey))
                {
                    ctx.SaveChangesWithRetries(SaveChangesOptions.Batch | SaveChangesOptions.ReplaceOnUpdate);
                    ctx = null;
                }

                if (ctx == null)
                {
                    dups.Clear();
                    lastPartitionKey = null;
                    ctx = tableClient.GetDataServiceContext();
                    ctx.WritingEntity += new EventHandler<ReadingWritingEntityEventArgs>(w.ctx_WritingEntity);
                    batchSize = 0;
                }

                // Add enty to the current batch
                // Upsert means insert+Replace. But still need uniqueness within a batch.
                bool allowUpsert = true;
                
                // Check for dups within a batch.
                var key = new PartitionRowKey { PartitionKey = entity.PartitionKey, RowKey = entity.RowKey };
                bool dupWithinBatch = dups.Contains(key);
                dups.Add(key);


                if (allowUpsert)
                {
                    // Upsert allows overwriting existing keys. But still must be unique within a batch.
                    if (!dupWithinBatch)
                    {
                        ctx.AttachTo(tableName, entity);
                        ctx.UpdateObject(entity);   
                    }
                }
                else
                {
                    // AddObject requires uniquess.
                    if (dupWithinBatch)
                    {
                        // Azure REST APIs will give us a horrible cryptic error (400 with no message). 
                        // Provide users a useful error instead.
                        throw new InvalidOperationException(string.Format("Table has duplicate keys: {0}", key));
                    }

                    ctx.AddObject(tableName, entity);
                }
                

                lastPartitionKey = entity.PartitionKey;
                batchSize++;

                if (batchSize % UploadBatchSize == 0)
                {
                    // Beware, if keys collide within a batch, we get a very cryptic error and 400.
                    // If they collide across batches, we get a more useful 409 (conflict). 
                    try
                    {
                        ctx.SaveChangesWithRetries(SaveChangesOptions.Batch | SaveChangesOptions.ReplaceOnUpdate);
                    }
                    catch (DataServiceRequestException de)
                    {
                        var e = de.InnerException as DataServiceClientException;
                        if (e != null)
                        {
                            if (e.StatusCode == 409)
                            {
                                // Conflict. Duplicate keys. We don't get the specific duplicate key.
                                // Server shouldn't do this if we support upsert.
                                // (although an old emulator that doesn't yet support upsert may throw it).
                                throw new InvalidOperationException(string.Format("Table has duplicate keys. {0}", e.Message));
                            }
                        }
                    }
                    ctx = null;
                }
            }

            if (ctx != null)
            {
                ctx.SaveChangesWithRetries(SaveChangesOptions.Batch | SaveChangesOptions.ReplaceOnUpdate);
            }
        }

        // Batches must be < 100. 
        // Larger batches are more efficient. 
        private const int UploadBatchSize = 50;

        private void ctx_WritingEntity(object sender, ReadingWritingEntityEventArgs args)
        {
            GenericWriterEntity entity = args.Entity as GenericWriterEntity;
            if (entity == null)
            {
                return;
            }

            XElement properties = args.Data.Descendants(GenericTableReader.MetadataNamespace + "properties").First();

            for(int iColumnn = 0; iColumnn < _edmTypeNames.Length; iColumnn++)
            {
                string edmTypeName = _edmTypeNames[iColumnn];
                if (edmTypeName == null)
                {
                    continue;
                }

                string value = entity._source.Values[iColumnn];                
                string columnName = _columnNames[iColumnn];

                // framework will handle row + partition keys. 
                XElement e = new XElement(GenericTableReader.DataNamespace + columnName, value);
                e.Add(new XAttribute(GenericTableReader.MetadataNamespace + "type", edmTypeName));

                properties.Add(e);
            }            
        }
     
        // Mapping of .NET types to EDM types.
        static Dictionary<Type, string> _edmNameMapping = new Dictionary<Type, string> { 
            { typeof(string), "Edm.String" },
            { typeof(byte), "Edm.Byte" },
            { typeof(sbyte), "Edm.SByte" },
            { typeof(short), "Edm.Int16" },
            { typeof(int), "Edm.Int32" },
            { typeof(long), "Edm.Int64" },
            { typeof(double), "Edm.Double" }, 
            { typeof(float), "Edm.Single" },
            { typeof(bool), "Edm.Boolean" },
            { typeof(decimal), "Edm.Decimal" },
            { typeof(DateTime), "Edm.DateTime" },
            { typeof(Guid), "Edm.Guid" }
        };

    }
}