using System.Globalization;
using CsvHelper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvReaderNode : AbstractSourceNode<Row>
    {
        private readonly bool _hasHeader;
        private readonly string _filePath;

        public CsvReaderNode(string filePath, bool hasHeaderRow = true)
        {
            _filePath = filePath;
            _hasHeader = hasHeaderRow;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            using var stream = File.OpenText(_filePath);
            using var reader = new CsvReader(stream, CultureInfo.InvariantCulture);

            //TODO: reader.Configuration.BadDataFound = null;
            reader.Read();
            if (_hasHeader)
                reader.ReadHeader();

            while (reader.Read())
            {
                var row = context.ObjectPool.Borrow<Row>();
                var values = Enumerable.Range(0, reader.HeaderRecord.Length - 1).Select(i => reader.GetField<object>(i))
                    .ToArray();
                row.Load(reader.HeaderRecord, values);
                Emit(row);
            }

            SignalEnd();
        }
    }
}