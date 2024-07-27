using System.Globalization;
using CsvHelper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvReaderNode : AbstractSourceNode<Row>
    {
        private readonly bool _hasHeader;
        private readonly CultureInfo _culture;
        private readonly string _filePath;

        public CsvReaderNode(string filePath, bool hasHeaderRow = true, CultureInfo culture = null)
        {
            _filePath = filePath;
            _hasHeader = hasHeaderRow;
            _culture = culture ?? CultureInfo.InvariantCulture;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            using var stream = File.OpenText(_filePath);
            using var reader = new CsvReader(stream, _culture);
            
            reader.Read();
            if (_hasHeader)
                reader.ReadHeader();

            while (reader.Read())
            {
                var row = context.ObjectPool.Borrow<Row>();
                row.Load(reader.HeaderRecord, reader.GetFields());
                Emit(row);
            }

            SignalEnd();
        }
    }
}