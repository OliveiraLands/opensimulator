/*
 * Copyright (c) Contributors, http://opensimulator.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the OpenSimulator Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using log4net;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using OpenMetaverse.ImportExport.Collada14;
using System.Data.Common;

namespace OpenSim.Data.MongoDB
{
    public class MongoDBGenericTableHandler<T> : MongoDBFramework where T : class, new()
    {
        private static readonly ILog m_log =
            LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected string m_ConnectionString;
        protected MongoDBManager m_database; //used for parameter type translation
        protected Dictionary<string, FieldInfo> m_Fields =
                new Dictionary<string, FieldInfo>();

        protected Dictionary<string, string> m_FieldTypes = new Dictionary<string, string>();

        protected List<string> m_ColumnNames = null;
        protected string m_Realm;
        protected FieldInfo m_DataField = null;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        public MongoDBGenericTableHandler(string connectionString,
                string realm, string storeName)
            : base(connectionString)
        {
            m_Realm = realm;

            m_ConnectionString = connectionString;

            if (storeName != System.String.Empty)
            {
                /*
                using (NpgsqlConnection conn = new NpgsqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    Migration m = new Migration(conn, GetType().Assembly, storeName);
                    m.Update();
                }
                */

            }
            m_database = new MongoDBManager(m_ConnectionString);

            Type t = typeof(T);
            FieldInfo[] fields = t.GetFields(BindingFlags.Public |
                                             BindingFlags.Instance |
                                             BindingFlags.DeclaredOnly);

            LoadFieldTypes();

            if (fields.Length == 0)
                return;

            foreach (FieldInfo f in fields)
            {
                if (f.Name != "Data")
                    m_Fields[f.Name] = f;
                else
                    m_DataField = f;
            }

        }

        private void LoadFieldTypes()
        {
            // Inicializa o dicionário de tipos de campo
            m_FieldTypes = new Dictionary<string, string>();

            /*
            // Conecta-se ao MongoDB
            var client = new MongoClient(m_ConnectionString);
            var database = client.GetDatabase("nome_do_banco"); // substitua pelo seu nome de banco de dados
            var collection = database.GetCollection<BsonDocument>(m_Realm.ToLower()); // substitua pelo nome da sua coleção

            // Busca um documento de amostra para inspecionar os tipos dos campos
            var sampleDocument = collection.Find(new BsonDocument()).FirstOrDefault();

            if (sampleDocument != null)
            {
                foreach (var element in sampleDocument)
                {
                    // Obtém o nome do campo e o tipo
                    string fieldName = element.Name;
                    string fieldType = element.Value.BsonType.ToString();

                    // Armazena no dicionário
                    m_FieldTypes.Add(fieldName, fieldType);
                }
            }
*/
        }

        /*
        private void CheckColumnNames(NpgsqlDataReader reader)
        {
            if (m_ColumnNames != null)
                return;

            m_ColumnNames = new List<string>();

            DataTable schemaTable = reader.GetSchemaTable();

            foreach (DataRow row in schemaTable.Rows)
            {
                if (row["ColumnName"] != null &&
                        (!m_Fields.ContainsKey(row["ColumnName"].ToString())))
                    m_ColumnNames.Add(row["ColumnName"].ToString());

            }
        }
        */

        // TODO GET CONSTRAINTS FROM POSTGRESQL
        private List<string> GetConstraints()
        {
            /*
            List<string> constraints = new List<string>();
            string query = string.Format(@"select
                    a.attname as column_name
                from
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a
                where
                    t.oid = ix.indrelid
                    and i.oid = ix.indexrelid
                    and a.attrelid = t.oid
                    and a.attnum = ANY(ix.indkey)
                    and t.relkind = 'r'
                    and ix.indisunique = true
                    and t.relname = lower('{0}')
            ;", m_Realm);

            using (NpgsqlConnection conn = new NpgsqlConnection(m_ConnectionString))
            using (NpgsqlCommand cmd = new NpgsqlCommand(query, conn))
            {
                conn.Open();
                using (NpgsqlDataReader rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        // query produces 0 to many rows of single column, so always add the first item in each row
                        constraints.Add((string)rdr[0]);
                    }
                }
                return constraints;
            }
            */
            return new List<string>();
        }

        public virtual T[] Get(string field, string key)
        {
            // Conectar-se ao MongoDB
            var client = new MongoClient(m_ConnectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName()); 
            var collection = database.GetCollection<T>(m_Realm.ToLower()); // Substitua pelo nome da sua coleção

            // Construir o filtro para a consulta usando o campo e a chave fornecidos
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(field, key);

            // Executar a consulta e retornar o resultado como um array
            List<T> results = collection.Find(filter).ToList();

            return results.ToArray();
        }

        public virtual T[] Get(string field, string[] keys)
        {
            if (keys == null || keys.Length == 0)
                return new T[0];

            // Conectar-se ao MongoDB
            var client = new MongoClient(m_ConnectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName()); // Substitua pelo seu banco de dados
            var collection = database.GetCollection<T>(m_Realm.ToLower()); // Substitua pelo nome da coleção

            // Construir o filtro usando o operador "IN" do MongoDB
            var filter = Builders<T>.Filter.In(field, keys);

            // Executar a consulta e retornar os resultados como um array
            List<T> results = collection.Find(filter).ToList();

            return results.ToArray();
        }

        public virtual T[] Get(string[] fields, string[] keys)
        {
            if (fields.Length != keys.Length || fields.Length == 0)
                return new T[0];

            // Conectar-se ao MongoDB
            var client = new MongoClient(m_ConnectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());
            var collection = database.GetCollection<T>(m_Realm.ToLower()); // Substitua pelo nome da coleção

            // Construir o filtro para combinar as condições de "campo = valor" com "AND"
            var filterBuilder = Builders<T>.Filter;
            var filters = new List<FilterDefinition<T>>();

            for (int i = 0; i < fields.Length; i++)
            {
                filters.Add(filterBuilder.Eq(fields[i], keys[i]));
            }

            var finalFilter = filterBuilder.And(filters);

            // Executar a consulta e retornar os resultados como um array
            List<T> results = collection.Find(finalFilter).ToList();

            return results.ToArray();
        }

        protected T[] DoQuery(DbCommand cmd)
        {
            throw new NotImplementedException();
            /*
            List<T> result = new List<T>();
            if (cmd.Connection == null)
            {
                cmd.Connection = new NpgsqlConnection(m_connectionString);
            }
            if (cmd.Connection.State == ConnectionState.Closed)
            {
                cmd.Connection.Open();
            }
            using (NpgsqlDataReader reader = cmd.ExecuteReader())
            {
                if (reader == null)
                    return new T[0];

                CheckColumnNames(reader);

                while (reader.Read())
                {
                    T row = new T();

                    foreach (string name in m_Fields.Keys)
                    {
                        if (m_Fields[name].GetValue(row) is bool)
                        {
                            int v = Convert.ToInt32(reader[name]);
                            m_Fields[name].SetValue(row, v != 0 ? true : false);
                        }
                        else if (m_Fields[name].GetValue(row) is UUID)
                        {
                            UUID uuid = UUID.Zero;

                            UUID.TryParse(reader[name].ToString(), out uuid);
                            m_Fields[name].SetValue(row, uuid);
                        }
                        else if (m_Fields[name].GetValue(row) is int)
                        {
                            int v = Convert.ToInt32(reader[name]);
                            m_Fields[name].SetValue(row, v);
                        }
                        else
                        {
                            m_Fields[name].SetValue(row, reader[name]);
                        }
                    }

                    if (m_DataField != null)
                    {
                        Dictionary<string, string> data =
                                new Dictionary<string, string>();

                        foreach (string col in m_ColumnNames)
                        {
                            data[col] = reader[col].ToString();

                            if (data[col] == null)
                                data[col] = System.String.Empty;
                        }

                        m_DataField.SetValue(row, data);
                    }

                    result.Add(row);
                }
                return result.ToArray();
            }
            */
        }

        public virtual T[] Get(string where)
        {
            // Conectar-se ao MongoDB
            var client = new MongoClient(m_ConnectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());
            var collection = database.GetCollection<T>(m_Realm.ToLower()); // Substitua pelo nome da coleção

            // Criar um filtro para o MongoDB a partir da cláusula 'where'
            var filter = BuildFilterFromWhereClause(where);

            // Consultar os documentos que atendem ao filtro
            var results = collection.Find(filter).ToList();

            return results.ToArray();
        }

        private FilterDefinition<T> BuildFilterFromWhereClause(string where)
        {
            throw new NotImplementedException();
        }

        public virtual T[] Get(string where, DbParameter parameter)
        {
            throw new NotImplementedException();
            /*
            using (NpgsqlConnection conn = new NpgsqlConnection(m_ConnectionString))
                using (NpgsqlCommand cmd = new NpgsqlCommand())
            {

                string query = String.Format("SELECT * FROM {0} WHERE {1}",
                                             m_Realm, where);
                cmd.Connection = conn;
                cmd.CommandText = query;
                //m_log.WarnFormat("[PGSQLGenericTable]: SELECT {0} WHERE {1}", m_Realm, where);

                cmd.Parameters.Add(parameter);

                conn.Open();
                return DoQuery(cmd);
            }
            */
        }

        public virtual bool Store(T row)
        {
            throw new NotImplementedException();
            /*
            List<string> constraintFields = GetConstraints();
            List<KeyValuePair<string, string>> constraints = new List<KeyValuePair<string, string>>();

            using (NpgsqlConnection conn = new NpgsqlConnection(m_ConnectionString))
            using (NpgsqlCommand cmd = new NpgsqlCommand())
            {

                StringBuilder query = new StringBuilder();
                List<String> names = new List<String>();
                List<String> values = new List<String>();

                foreach (FieldInfo fi in m_Fields.Values)
                {
                    names.Add(fi.Name);
                    values.Add(":" + fi.Name);
                    // Temporarily return more information about what field is unexpectedly null for
                    // http://opensimulator.org/mantis/view.php?id=5403.  This might be due to a bug in the
                    // InventoryTransferModule or we may be required to substitute a DBNull here.
                    if (fi.GetValue(row) == null)
                        throw new NullReferenceException(
                            string.Format(
                                "[PGSQL GENERIC TABLE HANDLER]: Trying to store field {0} for {1} which is unexpectedly null",
                                fi.Name, row));

                    if (constraintFields.Count > 0 && constraintFields.Contains(fi.Name))
                    {
                        constraints.Add(new KeyValuePair<string, string>(fi.Name, fi.GetValue(row).ToString() ));
                    }

                    if (m_FieldTypes.TryGetValue(fi.Name, out string ftype))
                        cmd.Parameters.Add(m_database.CreateParameter(fi.Name, fi.GetValue(row), ftype));
                    else
                        cmd.Parameters.Add(m_database.CreateParameter(fi.Name, fi.GetValue(row)));
                }

                if (m_DataField != null)
                {
                    Dictionary<string, string> data =
                            (Dictionary<string, string>)m_DataField.GetValue(row);

                    foreach (KeyValuePair<string, string> kvp in data)
                    {
                        if (constraintFields.Count > 0 && constraintFields.Contains(kvp.Key))
                        {
                            constraints.Add(new KeyValuePair<string, string>(kvp.Key, kvp.Key));
                        }
                        names.Add(kvp.Key);
                        values.Add(":" + kvp.Key);

                        if (m_FieldTypes.TryGetValue(kvp.Key, out string ftype))
                            cmd.Parameters.Add(m_database.CreateParameter("" + kvp.Key, kvp.Value, ftype));
                        else
                            cmd.Parameters.Add(m_database.CreateParameter("" + kvp.Key, kvp.Value));
                    }

                }

                query.AppendFormat("UPDATE {0} SET ", m_Realm);
                int i = 0;
                for (i = 0; i < names.Count - 1; i++)
                {
                    query.AppendFormat("\"{0}\" = {1}, ", names[i], values[i]);
                }
                query.AppendFormat("\"{0}\" = {1} ", names[i], values[i]);
                if (constraints.Count > 0)
                {
                    List<string> terms = new List<string>();
                    for (int j = 0; j < constraints.Count; j++)
                    {
                        terms.Add(String.Format(" \"{0}\" = :{0}", constraints[j].Key));
                    }
                    string where = String.Join(" AND ", terms.ToArray());
                    query.AppendFormat(" WHERE {0} ", where);

                }
                cmd.Connection = conn;
                cmd.CommandText = query.ToString();

                conn.Open();
                if (cmd.ExecuteNonQuery() > 0)
                {
                    //m_log.WarnFormat("[PGSQLGenericTable]: Updating {0}", m_Realm);
                    return true;
                }
                else
                {
                    // assume record has not yet been inserted

                    query = new StringBuilder();
                    query.AppendFormat("INSERT INTO {0} (\"", m_Realm);
                    query.Append(String.Join("\",\"", names.ToArray()));
                    query.Append("\") values (" + String.Join(",", values.ToArray()) + ")");
                    cmd.Connection = conn;
                    cmd.CommandText = query.ToString();

                    // m_log.WarnFormat("[PGSQLGenericTable]: Inserting into {0} sql {1}", m_Realm, cmd.CommandText);

                    if (conn.State != ConnectionState.Open)
                        conn.Open();
                    if (cmd.ExecuteNonQuery() > 0)
                        return true;
                }

                return false;
            }
            */
        }

        public virtual bool Delete(string field, string key)
        {
            return Delete(new string[] { field }, new string[] { key });
        }

        public virtual bool Delete(string[] fields, string[] keys)
        {
            if (fields.Length != keys.Length || fields.Length == 0)
                return false;

            // Conectar-se ao MongoDB
            var client = new MongoClient(m_ConnectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());
            var collection = database.GetCollection<T>(m_Realm.ToLower()); // Substitua pelo nome da coleção

            // Construir o filtro para combinar as condições de "campo = valor" com "AND"
            var filterBuilder = Builders<T>.Filter;
            var filters = new List<FilterDefinition<T>>();

            for (int i = 0; i < fields.Length; i++)
            {
                filters.Add(filterBuilder.Eq(fields[i], keys[i]));
            }

            var finalFilter = filterBuilder.And(filters);

            // Executar a operação de exclusão e verificar o número de documentos excluídos
            var result = collection.DeleteMany(finalFilter);

            return result.DeletedCount > 0;
        }
        public long GetCount(string field, string key)
        {
            return GetCount(new string[] { field }, new string[] { key });
        }

        public long GetCount(string[] fields, string[] keys)
        {
            if (fields.Length != keys.Length || fields.Length == 0)
                return 0;

            // Conectar-se ao MongoDB
            var client = new MongoClient(m_ConnectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());
            var collection = database.GetCollection<T>(m_Realm.ToLower()); // Substitua pelo nome da coleção

            // Construir o filtro para contar documentos
            var filterBuilder = Builders<T>.Filter;
            var filters = new List<FilterDefinition<T>>();

            for (int i = 0; i < fields.Length; i++)
            {
                // Supondo que os campos são do tipo Guid, você pode converter as chaves
                if (Guid.TryParse(keys[i], out Guid guidKey))
                {
                    filters.Add(filterBuilder.Eq(fields[i], guidKey));
                }
            }

            var finalFilter = filterBuilder.And(filters);

            // Contar os documentos que atendem ao filtro
            long count = collection.CountDocuments(finalFilter);

            return count;
        }

        // Método auxiliar para construir um filtro a partir da cláusula 'where'
          private FilterDefinition<T> BuildFilterFromWhereClause(string where, FilterDefinitionBuilder<T> filterBuilder)
        {
            // Aqui você pode implementar uma lógica para converter a string 'where' em filtros do MongoDB
            // Por simplicidade, um exemplo básico para suportar "=" e "AND" é mostrado. 
            // Você deve expandir isso conforme necessário para suas necessidades específicas.

            var filters = new List<FilterDefinition<T>>();

            // Exemplo simples: suportar apenas "=" e "AND"
            var conditions = where.Split(new[] { " AND " }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var condition in conditions)
            {
                var parts = condition.Split(new[] { '=' }, 2);
                if (parts.Length == 2)
                {
                    var field = parts[0].Trim().Trim('"'); // Remover aspas
                    var value = parts[1].Trim().Trim('\''); // Remover aspas simples

                    // Você pode adicionar lógica aqui para converter o valor para o tipo apropriado se necessário
                    filters.Add(filterBuilder.Eq(field, value));
                }
            }

            return filterBuilder.And(filters);
        }

        public long GetCount(string where)
        {
            // Conectar-se ao MongoDB
            var client = new MongoClient(m_ConnectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());
            var collection = database.GetCollection<T>(m_Realm.ToLower()); // Substitua pelo nome da coleção

            // Criar um filtro para o MongoDB a partir da string 'where'
            var filterBuilder = Builders<T>.Filter;
            var filter = BuildFilterFromWhereClause(where, filterBuilder);

            // Contar os documentos que atendem ao filtro
            long count = collection.CountDocuments(filter);

            return count;
        }

        public object DoQueryScalar(DbCommand cmd)
        {
            throw new NotImplementedException();
            /*
            using (NpgsqlConnection dbcon = new NpgsqlConnection(m_ConnectionString))
            {
                dbcon.Open();
                cmd.Connection = dbcon;

                return cmd.ExecuteScalar();
            }
            */
        }
    }
}
