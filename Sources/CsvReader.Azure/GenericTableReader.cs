using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using Common = System.Data.Services.Common;
using Client = System.Data.Services.Client;

namespace DataAccess
{
    [Common.DataServiceKey("PartitionKey", "RowKey")]
    internal class GenericEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public Dictionary<string, string> properties = new Dictionary<string, string>();
    }

    internal class GenericTableReader
    {        
        public static XNamespace AtomNamespace = "http://www.w3.org/2005/Atom";
        public static XNamespace DataNamespace = "http://schemas.microsoft.com/ado/2007/08/dataservices";
        public static XNamespace MetadataNamespace = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
          
        // This manually parses the XML that comes back.
        // This function uses code from this blog entry:
        // http://blogs.msdn.com/b/avkashchauhan/archive/2011/03/28/reading-and-saving-table-storage-entities-without-knowing-the-schema-or-updating-tablestorageentity-schema-at-runtime.aspx
        public static void OnReadingEntity(object sender, Client.ReadingWritingEntityEventArgs args)
        {
            GenericEntity entity = args.Entity as GenericEntity;
            if (entity == null)
            {
                return;
            }

            // read each property, type and value in the payload   
            var properties = args.Entity.GetType().GetProperties();
            var q = from p in args.Data.Element(AtomNamespace + "content")
                                    .Element(MetadataNamespace + "properties")
                                    .Elements()
                    where properties.All(pp => pp.Name != p.Name.LocalName)
                    select new
                    {
                        Name = p.Name.LocalName,
                        IsNull = string.Equals("true", p.Attribute(MetadataNamespace + "null") == null ? null : p.Attribute(MetadataNamespace + "null").Value, StringComparison.OrdinalIgnoreCase),
                        TypeName = p.Attribute(MetadataNamespace + "type") == null ? null : p.Attribute(MetadataNamespace + "type").Value,
                        p.Value
                    };

            foreach (var dp in q)
            {
                string value = dp.Value;
                if (!string.IsNullOrWhiteSpace(value))
                {
                    value = string.Empty;
                }
                entity.properties[dp.Name] = dp.Value;
            }
        }
    }
}



