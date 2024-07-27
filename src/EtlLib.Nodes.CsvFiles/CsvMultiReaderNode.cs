using System.Globalization;
using CsvHelper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvMultiReaderNode : AbstractProcessingNode<NodeOutputWithFilePath, Row>
    {
        private bool _hasHeader;

        public CsvMultiReaderNode()
        {
            _hasHeader = true;
        }

        public CsvMultiReaderNode HasHeader(bool hasHeader = true)
        {
            _hasHeader = hasHeader;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var input in Input)
            {
                using var stream = File.OpenText(input.FilePath);
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
            }

            SignalEnd();
        }
    }
}