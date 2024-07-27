﻿using System.Data.SqlClient;
using EtlLib.Pipeline;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Xunit;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class DbConnectionFactoryTests
    {
        [Fact]
        public void Can_register_typed_connections()
        {
            const string cs1 = "Data Source =:memory:";
            const string cs2 = "Data Source=:memory:";
            const string cs3 = "Server=myServerAddress;Database=myDataBase;Trusted_Connection=True;";

            var registrar = new DbConnectionFactory();

            ((IDbConnectionRegistrar) registrar)
                .For<SqliteConnection>(con => con
                    .Register("inmemory", cs1)
                    .Register("inmemory2", cs2))
                .For<SqlConnection>(con => con
                    .Register("remotedb", cs3));

            var connection1 = ((IDbConnectionFactory) registrar)
                .CreateNamedConnection("inmemory");

            connection1.Should().NotBeNull();
            connection1.ConnectionString.Should().Be(cs1);
            connection1.Should().BeOfType<SqliteConnection>();

            var connection2 = ((IDbConnectionFactory)registrar)
                .CreateNamedConnection("inmemory2");

            connection2.Should().NotBeNull();
            connection2.ConnectionString.Should().Be(cs2);
            connection2.Should().BeOfType<SqliteConnection>();

            var connection3 = ((IDbConnectionFactory)registrar)
                .CreateNamedConnection("remotedb");

            connection3.Should().NotBeNull();
            connection3.ConnectionString.Should().Be(cs3);
            connection3.Should().BeOfType<SqlConnection>();
        }

        [Fact]
        public void Can_register_and_resolve_via_pipeline_context()
        {
            const string cs1 = "Data Source =:memory:";

            var context = new EtlPipelineContext();

            context.DbConnections
                .For<SqliteConnection>(con => con.Register("inmemory", cs1));

            var connection1 = context.CreateNamedDbConnection("inmemory");

            connection1.Should().NotBeNull();
            connection1.ConnectionString.Should().Be(cs1);
            connection1.Should().BeOfType<SqliteConnection>();
        }
    }
}