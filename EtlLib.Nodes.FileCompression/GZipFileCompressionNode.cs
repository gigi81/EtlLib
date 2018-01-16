﻿using System;
using System.IO;
using System.Threading.Tasks;
using EtlLib.Data;
using ICSharpCode.SharpZipLib.GZip;

namespace EtlLib.Nodes.FileCompression
{
    public class GZipFileCompressionNode : AbstractInputOutputNode<NodeOutputWithFilePath, NodeOutputWithFilePath>
    {
        private int _compressionLevel, _degreeOfParallelism;
        private string _fileSuffix;

        public GZipFileCompressionNode()
        {
            _compressionLevel = 5;
            _degreeOfParallelism = 1;
            _fileSuffix = ".gz";
        }

        /// <summary>
        /// Block size acts as the compression level (1 to 9) with 1 being the lowest compression and 9 being the highest.
        /// </summary>
        /// <param name="compressionLevel">The block size (compression level), 1 to 9.</param>
        public GZipFileCompressionNode CompressionLevel(int compressionLevel)
        {
            if (compressionLevel < 1 || compressionLevel > 9)
                throw new ArgumentException("BZip2 compression level must be between 1 and 9.", nameof(compressionLevel));

            _compressionLevel = compressionLevel;
            return this;
        }

        public GZipFileCompressionNode Parallelize(int degreeOfParallelism)
        {
            _degreeOfParallelism = degreeOfParallelism;
            return this;
        }

        public GZipFileCompressionNode FileSuffix(string suffix)
        {
            _fileSuffix = suffix;
            return this;
        }

        public override void OnExecute()
        {
            Parallel.ForEach(Input, new ParallelOptions { MaxDegreeOfParallelism = _degreeOfParallelism }, item =>
            {
                var outFileName = item.FilePath + _fileSuffix;
                GZip.Compress(File.OpenRead(item.FilePath), File.OpenWrite(outFileName), true, _compressionLevel);
                Emit(new NodeOutputWithFilePath(outFileName));
            });

            SignalEnd();
        }
    }
}