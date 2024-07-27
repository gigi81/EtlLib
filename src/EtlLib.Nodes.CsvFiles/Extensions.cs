using CsvHelper;

namespace EtlLib.Nodes.CsvFiles;

public static class Extensions
{
    public static object[] GetFields(this CsvReader reader)
    {
        var ret = new object[reader.ColumnCount];

        for (var i = 0; i < reader.ColumnCount; i++)
        {
            ret[i] = reader.GetField(i);
        }
        
        return ret;
    }
}