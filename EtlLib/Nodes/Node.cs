﻿using System;
using EtlLib.Pipeline;

namespace EtlLib.Nodes
{
    public abstract class Node : INode
    {
        public Guid Id { get; private set; }
        public EtlProcessContext Context { get; private set; }
        public INodeWaiter Waiter { get; private set; }

        public INode SetId(Guid id)
        {
            Id = id;
            return this;
        }

        public INode SetContext(EtlProcessContext context)
        {
            Context = context;
            return this;
        }

        public INode SetWaiter(INodeWaiter waiter)
        {
            Waiter = waiter;
            return this;
        }

        public void Execute()
        {
            Waiter?.Wait();

            OnExecute();
        }

        public abstract void OnExecute();

        public override string ToString()
        {
            return $"{GetType().Name}=({Id})";
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        protected bool Equals(Node other)
        {
            return Id.Equals(other.Id);
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }
    }

    public abstract class BlockingNode : Node, IBlockingNode
    {
        public INodeWaitSignaller WaitSignaller { get; private set; }
        public INode SetWaitSignaller(INodeWaitSignaller signaller)
        {
            WaitSignaller = signaller;
            return this;
        }
    }
}