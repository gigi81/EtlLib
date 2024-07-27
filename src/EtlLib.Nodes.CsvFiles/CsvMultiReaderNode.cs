using System.Globalization;
using CsvHelper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvMultiReaderNode : AbstractProcessingNode<NodeOutputWithFilePath, Row>
    {
        private bool _hasHeader = true;
        private CultureInfo _culture = CultureInfo.InvariantCulture;

        public CsvMultiReaderNode HasHeader(bool hasHeader = true)
        {
            _hasHeader = hasHeader;
            return this;
        }

        public CsvMultiReaderNode WithCulture(CultureInfo culture)
        {
            _culture = culture;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var input in Input)
            {
                using var stream = File.OpenText(input.FilePath);
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
            }

            SignalEnd();
        }
    }
}