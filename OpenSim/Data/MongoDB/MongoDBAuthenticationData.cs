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
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ''AS IS'' AND ANY
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
using System.Collections;
using System.Collections.Generic;
using OpenMetaverse;
using OpenSim.Framework;
using System.Reflection;
using System.Text;
using System.Data;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;

namespace OpenSim.Data.MongoDB
{
    public class MongoDBAuthenticationData : IAuthenticationData
    {
        private string m_Realm;
        private List<string> m_ColumnNames = null;
        private int m_LastExpire = 0;
        private string m_ConnectionString;
        private MongoDBManager m_database;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        public MongoDBAuthenticationData(string connectionString, string realm)
        {
            m_Realm = realm;
            m_ConnectionString = connectionString;
            m_database = new MongoDBManager(m_ConnectionString);
            /*
            using (NpgsqlConnection conn = new NpgsqlConnection(m_ConnectionString))
            {
                conn.Open();
                Migration m = new Migration(conn, GetType().Assembly, "AuthStore");
                
                m.Update();
            }
            */
        }

        public AuthenticationData Get(UUID principalID)
        {
            var ret = new AuthenticationData
            {
                Data = new Dictionary<string, object>()
            };

            var mongoClient = new MongoClient(m_ConnectionString);
            var db = mongoClient.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<BsonDocument>(m_Realm);

            // Definindo o filtro para buscar o documento pelo UUID
            var filter = Builders<BsonDocument>.Filter.Eq("uuid", principalID.ToString());
            var document = collection.Find(filter).FirstOrDefault();

            if (document != null)
            {
                ret.PrincipalID = principalID;

                if (m_ColumnNames == null)
                {
                    m_ColumnNames = new List<string>(document.Names);
                }

                foreach (string s in m_ColumnNames)
                {
                    if (s == "UUID" || s == "uuid")
                        continue;

                    ret.Data[s] = document[s].ToString();
                }

                return ret;
            }

            return null;
        }

        public bool Store(AuthenticationData data)
        {
            // Remover campos indesejados
            data.Data.Remove("UUID");
            data.Data.Remove("uuid");

            // Criar ou atualizar o documento no MongoDB
            var mongoClient = new MongoClient(m_ConnectionString);
            var db = mongoClient.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<BsonDocument>(m_Realm);

            // Criar o documento para armazenar
            var document = new BsonDocument
    {
        { "uuid", data.PrincipalID.ToString() } // Adiciona o UUID
    };

            // Adicionar os dados ao documento
            foreach (var kvp in data.Data)
            {
                document[kvp.Key.ToLower()] = BsonValue.Create(kvp.Value); // Converter valores para Bson
            }

            // Tentar atualizar um documento existente
            var filter = Builders<BsonDocument>.Filter.Eq("uuid", data.PrincipalID.ToString());
            var updateResult = collection.ReplaceOne(filter, document, new ReplaceOptions { IsUpsert = true });

            return updateResult.IsAcknowledged;
        }

        public bool SetDataItem(UUID principalID, string item, string value)
        {
            var mongoClient = new MongoClient(m_ConnectionString);
            var db = mongoClient.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<BsonDocument>(m_Realm);

            // Criar um filtro para encontrar o documento pelo UUID
            var filter = Builders<BsonDocument>.Filter.Eq("uuid", principalID.ToString());

            // Criar a atualização para o campo especificado
            var update = Builders<BsonDocument>.Update.Set(item.ToLower(), value); // Pode querer garantir que o item esteja em minúsculas

            // Executar a atualização
            var result = collection.UpdateOne(filter, update);

            return result.ModifiedCount > 0; // Retorna true se o documento foi modificado
        }
        

        public bool SetToken(UUID principalID, string token, int lifetime)
        {
            if (System.Environment.TickCount - m_LastExpire > 30000)
                DoExpire();

            var mongoClient = new MongoClient(m_ConnectionString);
            var db = mongoClient.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<TokenDocumentType>("tokens"); // Substitua 'TokenDocumentType' pelo tipo de documento correto

            var tokenEntry = new TokenDocumentType
            {
                UUID = principalID,
                Token = token,
                Validity = DateTime.Now.AddMinutes(lifetime)
            };

            collection.InsertOne(tokenEntry);
            return true;
        }

        public bool CheckToken(UUID principalID, string token, int lifetime)
        {
            if (System.Environment.TickCount - m_LastExpire > 30000)
                DoExpire();

            DateTime validDate = DateTime.Now.AddMinutes(lifetime);

            var mongoClient = new MongoClient(m_ConnectionString);
            var db = mongoClient.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<TokenDocumentType>("tokens"); // Substitua 'TokenDocumentType' pelo tipo de documento correto

            var filter = Builders<TokenDocumentType>.Filter.And(
                Builders<TokenDocumentType>.Filter.Eq(t => t.UUID, principalID),
                Builders<TokenDocumentType>.Filter.Eq(t => t.Token, token),
                Builders<TokenDocumentType>.Filter.Gt(t => t.Validity, DateTime.Now) // Verifica se a validade é maior que a data atual
            );

            var update = Builders<TokenDocumentType>.Update.Set(t => t.Validity, validDate);

            var result = collection.UpdateOne(filter, update);

            return result.ModifiedCount > 0;
        }

        private void DoExpire()
        {
            DateTime currentDateTime = DateTime.Now;

            var mongoClient = new MongoClient(m_ConnectionString);
            var db = mongoClient.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<TokenDocumentType>("tokens"); // Substitua 'TokenDocumentType' pelo tipo de documento correto

            var filter = Builders<TokenDocumentType>.Filter.Lt(t => t.Validity, currentDateTime); // Filtra tokens com validade anterior à data atual

            // Remove os documentos que correspondem ao filtro
            collection.DeleteMany(filter);

            m_LastExpire = System.Environment.TickCount;
        }
    }

    public class TokenDocumentType
    {
        public UUID UUID { get; set; }
        public string Token { get; set; }
        public DateTime Validity { get; set; }
    }

}
