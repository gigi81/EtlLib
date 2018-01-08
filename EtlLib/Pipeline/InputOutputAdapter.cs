﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public interface IInputOutputAdapter : IDisposable
    {
        INode OutputNode { get; }

        bool AttachConsumer<T>(INodeWithInput<T> input) 
            where T : class, INodeOutput<T>, new();

        bool AttachConsumer<T>(INodeWithInput2<T> input)
            where T : class, INodeOutput<T>, new();
    }

    public class InputOutputAdapter<T> : IInputOutputAdapter, IEmitter<T>
        where T : class, INodeOutput<T>, new()
    {
        private readonly ConcurrentDictionary<Guid, BlockingCollection<T>> _queueMap;
        private readonly ConcurrentBag<INodeWithInput<T>> _inputs;
        private readonly INodeWithOutput<T> _output;
        private readonly EtlProcessContext _context;
        private ILogger _log;
        private long _emittedItems;

        public INode OutputNode => _output;
        public INodeWaitSignaller WaitSignaller { get; }
        public INodeWaiter Waiter { get; }

        public InputOutputAdapter(EtlProcessContext context, INodeWithOutput<T> output)
        {
            _queueMap = new ConcurrentDictionary<Guid, BlockingCollection<T>>();
            _inputs = new ConcurrentBag<INodeWithInput<T>>();
            _output = output;
            _log = new NullLogger();
            _context = context;

            if (output is IBlockingNode)
            {
                var signaller = new BlockingWaitSignaller();
                ((IBlockingNode)output).SetWaitSignaller(WaitSignaller);
                Waiter = signaller;
                WaitSignaller = signaller;
            }
            else
            {
                Waiter = new NoWaitNodeWaiter();
            }
        }

        public InputOutputAdapter<T> WithLogger(ILogger log)
        {
            _log = log;
            return this;
        }

        public IEnumerable<T> GetConsumingEnumerable(Guid nodeId)
        {
            return _queueMap[nodeId].GetConsumingEnumerable();
        }

        public IEnumerable<T> GetConsumingEnumerable(INodeWithInput<T> node)
        {
            return _queueMap[node.Id].GetConsumingEnumerable();
        }

        public bool AttachConsumer<TIn>(INodeWithInput<TIn> input)
            where TIn : class, INodeOutput<TIn>, new()
        {
            if (input.Input != null)
                throw new InvalidOperationException($"Node (Id={input.Id}, Type={input.GetType().Name}) already has an input assigned.");

            if (!_queueMap.TryAdd(input.Id, new BlockingCollection<T>(new ConcurrentQueue<T>())))
                return false;

            _inputs.Add((INodeWithInput<T>)input);

            return true;
        }

        public bool AttachConsumer<TIn>(INodeWithInput2<TIn> input)
            where TIn : class, INodeOutput<TIn>, new()
        {
            if (input.Input != null && input.Input2 != null)
                throw new InvalidOperationException($"Node (Id={input.Id}, Type={input.GetType().Name}) has two input slots of which both are already assigned.");

            if (!_queueMap.TryAdd(input.Id, new BlockingCollection<T>(new ConcurrentQueue<T>())))
                return false;

            _inputs.Add((INodeWithInput<T>)input);
            return true;
        }

        public void Emit(T item)
        {
            if (_emittedItems == 0)
                _log.Debug($"Node {_output} emitting its first item.");

            item.Freeze();
            _emittedItems++;

            var firstTarget = true;
            foreach (var queue in _queueMap.Values)
            {
                if (firstTarget)
                {
                    queue.Add(item);
                    firstTarget = false;
                }
                else
                {
                    var duplicatedItem = _context.ObjectPool.Borrow<T>();
                    item.CopyTo(duplicatedItem);
                    queue.Add(duplicatedItem);
                }
            }

            if (_emittedItems % 5000 == 0)
                _log.Debug($"Node {_output} has emitted {_emittedItems} items.");
        }

        public void SignalEnd()
        {
            _log.Debug($"Node {_output} has signalled the end of its data stream (emitted {_emittedItems} total items).");

            foreach (var queue in _queueMap.Values)
                queue.CompleteAdding();
        }

        public void Dispose()
        {
            foreach(var buffer in _queueMap)
                buffer.Value.Dispose();
        }
    }

    public class BlockingWaitSignaller : INodeWaitSignaller, INodeWaiter
    {
        private readonly ManualResetEventSlim _resetEvent;

        public BlockingWaitSignaller()
        {
            _resetEvent = new ManualResetEventSlim();
        }

        public void SignalWaitEnd()
        {
            _resetEvent.Set();
        }

        public void Wait()
        {
            _resetEvent.Wait();
        }

        public void Dispose()
        {
            _resetEvent?.Dispose();
        }
    }

    public class NoWaitNodeWaiter : INodeWaiter
    {
        public static NoWaitNodeWaiter Instance;

        static NoWaitNodeWaiter()
        {
            Instance = new NoWaitNodeWaiter();
        }

        public void Wait()
        {
        }
    }
}