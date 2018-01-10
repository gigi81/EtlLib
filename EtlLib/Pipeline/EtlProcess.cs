﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Pipeline.Builders;

namespace EtlLib.Pipeline
{
    public class EtlProcess
    {
        private readonly EtlProcessContext _processContext;
        private readonly ILogger _log;
        private readonly List<IInputOutputAdapter> _ioAdapters;
        private readonly List<string> _attachmentDeduplicationList;
        private readonly List<INode> _nodes;
        private readonly EtlProcessSettings _settings;
        private readonly NodeStatistics _nodeStatistics;

        public INodeWithOutput RootNode { get; }
        public string Name { get; }

        public EtlProcess(EtlProcessSettings settings, EtlProcessContext context)
        {
            _settings = settings;

            Name = settings.Name;

            _ioAdapters = new List<IInputOutputAdapter>();
            _attachmentDeduplicationList = new List<string>();
            _nodes = new List<INode>();
            _nodeStatistics = new NodeStatistics();
            
            _processContext = context;

            _log = settings.LoggingAdapter.CreateLogger("EtlLib.EtlProcess");
        }

        private void RegisterNode(INode node)
        {
            if (_nodes.Contains(node))
                return;

            node.SetContext(_processContext);
            _nodes.Add(node);
            _nodeStatistics.RegisterNode(node);
        }

        private void RegisterNodes(params INode[] nodes)
        {
            foreach(var node in nodes)
                RegisterNode(node);
        }

        public void AttachInputToOutput<T>(INodeWithOutput<T> output, INodeWithInput<T> input) 
            where T : class, INodeOutput<T>, new()
        {
            var dedupHash = $"{output.Id}:{input.Id}";

            if (_attachmentDeduplicationList.Contains(dedupHash))
            {
                _log.Debug($"Node {input} is already attached to output {output}");
                return;
            }

            RegisterNodes(input, output);

            if (!(_ioAdapters.SingleOrDefault(x => x.OutputNode.Equals(output)) is InputOutputAdapter<T> ioAdapter))
            {
                ioAdapter = new InputOutputAdapter<T>(_processContext, output, _nodeStatistics)
                    .WithLogger(_settings.LoggingAdapter.CreateLogger("EtlLib.IOAdapter"));

                output.SetEmitter(ioAdapter);

                _ioAdapters.Add(ioAdapter);
            }

            if (input is INodeWithInput2<T> input2)
            {
                ioAdapter.AttachConsumer(input2);

                if (input2.Input == null)
                {
                    _log.Info($"Attaching {input} input port #1 to output port of {output}.");
                    input.SetInput(ioAdapter.GetConsumingEnumerable(input));
                }
                else if (input2.Input2 == null)
                {
                    _log.Info($"Attaching {input} input port #2 to output port of {output}.");
                    input2.SetInput2(ioAdapter.GetConsumingEnumerable(input2));
                }
            }
            else
            {
                ioAdapter.AttachConsumer(input);

                _log.Info($"Attaching {input} input port #1 to output port of {output}.");
                input.SetInput(ioAdapter.GetConsumingEnumerable(input));
            }

            input.SetWaiter(ioAdapter.Waiter);
            
            _attachmentDeduplicationList.Add($"{output.Id}:{input.Id}");
        }

        public void Execute()
        {
            _log.Info($"=== Executing ETL Process '{Name}' (Started {DateTime.Now}) ===");

            if (_settings.ContextInitializer != null)
            {
                _log.Info("Running context initializer.");
                _settings.ContextInitializer(_processContext);
            }

            if (_settings.ObjectPoolRegistrations.Count > 0)
            {
                _log.Info("Initializing object pools...");
                foreach (var pool in _settings.ObjectPoolRegistrations)
                {
                    _log.Info($" - ObjectPool<{pool.Type.Name}> (InitialSize={pool.InitialSize}, AutoGrow={pool.AutoGrow})");
                    _processContext.ObjectPool.RegisterAndInitializeObjectPool(pool.Type, pool.InitialSize, pool.AutoGrow);
                }
            }

            var elapsedDict = new ConcurrentDictionary<INode, TimeSpan>();

            var tasks = new List<Task>();
            var processStopwatch = Stopwatch.StartNew();


            foreach (var node in _nodes)
            {
                var task = Task.Run(() =>
                    {
                        _log.Info($"Beginning execute task for node {node}.");
                        var sw = Stopwatch.StartNew();
                        node.Execute();
                        sw.Stop();
                        _log.Info($"Execute task for node {node} has completed in {sw.Elapsed}.");
                        elapsedDict[node] = sw.Elapsed;
                    });

                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());

            processStopwatch.Stop();

            _log.Info($"=== ETL Process '{Name}' has completed (Runtime {processStopwatch.Elapsed}) ===");

            _log.Info("Execution statisticts:");
            var elapsedStats = elapsedDict.OrderBy(x => x.Value.TotalMilliseconds).ToArray();
            for (var i = 0; i < elapsedStats.Length; i++)
            {
                var sb = new StringBuilder();
                sb.Append($" * {elapsedStats[i].Key} => Elapsed: {elapsedStats[i].Value}");
                var ioAdapter = _ioAdapters.SingleOrDefault(x => x.OutputNode == elapsedStats[i].Key);

                var nodeStats = _nodeStatistics.GetNodeStatistics(elapsedStats[i].Key);
                sb.Append($" [R={nodeStats.Reads}, W={nodeStats.Writes}, E={nodeStats.Errors}]");

                if (i == 0)
                    sb.Append(" (fastest)");
                else if (i == elapsedStats.Length - 1)
                    sb.Append(" (slowest)");
                
                _log.Info(sb.ToString());
            }

            _log.Debug("Disposing of all input/output adapters.");
            _ioAdapters.ForEach(x => x.Dispose());

            _log.Debug("Deallocating all object pools.");
            _processContext.ObjectPool.DeAllocate();

            _log.Debug("Performing garbage collection of all generations.");
            GC.Collect();
        }
    }

    public class NodeStatistics
    {
        private readonly Dictionary<INode, SingleNodeStatistics> _stats;

        public NodeStatistics()
        {
            _stats = new Dictionary<INode, SingleNodeStatistics>();
        }

        public void IncrementReads(INode node) => _stats[node].IncrementReads();
        public void IncrementWrites(INode node) => _stats[node].IncrementWrites();
        public void IncrementErrors(INode node) => _stats[node].IncrementErrors();
        public void RegisterNode(INode node) => _stats[node] = new SingleNodeStatistics();
        public SingleNodeStatistics GetNodeStatistics(INode node) => _stats[node];
        

        public class SingleNodeStatistics
        {
            private volatile uint _reads, _writes, _errors;
            
            public uint Reads => _reads;
            public uint Writes => _writes;
            public uint Errors => _errors;

            public SingleNodeStatistics()
            {
                _reads = _writes = _errors = 0;
            }

            public void IncrementReads()
            {
                _reads++;
            }

            public void IncrementWrites()
            {
                _writes++;
            }

            public void IncrementErrors()
            {
                _errors++;
            }
        }
    }
}