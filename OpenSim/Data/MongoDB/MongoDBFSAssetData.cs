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
using System.Reflection;
using System.Collections.Generic;
using System.Data;
using OpenSim.Framework;
using OpenSim.Framework.Console;
using log4net;
using OpenMetaverse;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using System.Linq;

namespace OpenSim.Data.MongoDB
{
    public class MongoDBFSAssetData : IFSAssetDataPlugin
    {
        private const string _migrationStore = "FSAssetStore";
        private static string m_Table = "fsassets";
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private long m_ticksToEpoch;

        private MongoDBManager m_database;
        private MongoClient _mongoClient;
        private IMongoDatabase _db;
        private string m_connectionString;

        public MongoDBFSAssetData()
        {
        }

        public void Initialise(string connect, string realm, int UpdateAccessTime)
        {
            DaysBetweenAccessTimeUpdates = UpdateAccessTime;

            m_ticksToEpoch = new System.DateTime(1970, 1, 1).Ticks;

            m_connectionString = connect;
            m_database = new MongoDBManager(m_connectionString);
            _mongoClient = new MongoClient(m_connectionString);
            _db = _mongoClient.GetDatabase(m_database.GetDatabaseName());

            //New migration to check for DB changes
            //m_database.CheckMigration(_migrationStore);
        }

        public void Initialise()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Number of days that must pass before we update the access time on an asset when it has been fetched
        /// Config option to change this is "DaysBetweenAccessTimeUpdates"
        /// </summary>
        private int DaysBetweenAccessTimeUpdates = 0;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }
        
        #region IPlugin Members

        public string Version { get { return "1.0.0.0"; } }

        public void Dispose() { }

        public string Name
        {
            get { return "PGSQL FSAsset storage engine"; }
        }

        #endregion

        #region IFSAssetDataPlugin Members

        public AssetMetadata Get(string id, out string hash)
        {
            hash = string.Empty;
            AssetMetadata meta = null;
            UUID uuid = new UUID(id);

            var collection = _db.GetCollection<BsonDocument>(m_Table);
            var filter = Builders<BsonDocument>.Filter.Eq("id", uuid.ToString());

            var document = collection.Find(filter).FirstOrDefault();

            if (document != null)
            {
                meta = new AssetMetadata
                {
                    ID = id,
                    FullID = uuid,
                    Name = string.Empty,
                    Description = string.Empty,
                    Type = (sbyte)document["type"].AsInt32,
                    ContentType = SLUtil.SLAssetTypeToContentType((sbyte)document["type"].AsInt32),
                    CreationDate = Util.ToDateTime(document["create_time"].AsInt32),
                    Flags = (AssetFlags)document["asset_flags"].AsInt32
                };

                hash = document["hash"].AsString;

                int accessTime = document["access_time"].AsInt32;
                UpdateAccessTime(accessTime, uuid);
            }

            return meta;
        }

        private void UpdateAccessTime(int AccessTime, UUID id)
        {
            // Reduz o trabalho no banco de dados, atualizando o tempo de acesso somente se o ativo não foi acessado recentemente
            if (DaysBetweenAccessTimeUpdates > 0 && (DateTime.UtcNow - Utils.UnixTimeToDateTime(AccessTime)).TotalDays < DaysBetweenAccessTimeUpdates)
                return;

            var collection = _db.GetCollection<BsonDocument>(m_Table);
            int now = (int)((DateTime.UtcNow.Ticks - m_ticksToEpoch) / 10000000);

            var filter = Builders<BsonDocument>.Filter.Eq("id", id.ToString());
            var update = Builders<BsonDocument>.Update.Set("access_time", now);

            collection.UpdateOne(filter, update);
        }

        public bool Store(AssetMetadata meta, string hash)
        {
            try
            {
                var collection = _db.GetCollection<BsonDocument>(m_Table);
                bool found = false;
                int now = (int)((DateTime.UtcNow.Ticks - m_ticksToEpoch) / 10000000);

                // Procura o ativo existente
                var filter = Builders<BsonDocument>.Filter.Eq("id", meta.FullID.ToString());
                var existingAsset = collection.Find(filter).FirstOrDefault();

                if (existingAsset == null)
                {
                    // Insere um novo documento, pois o ativo não existe
                    var newAsset = new BsonDocument
            {
                { "id", meta.FullID.ToString() },
                { "type", meta.Type },
                { "hash", hash },
                { "asset_flags", Convert.ToInt32(meta.Flags) },
                { "create_time", now },
                { "access_time", now }
            };
                    collection.InsertOne(newAsset);
                    found = true;
                }
                else
                {
                    // Atualiza o tempo de acesso do ativo existente
                    var update = Builders<BsonDocument>.Update
                        .Set("access_time", now)
                        .Set("hash", hash)
                        .Set("type", meta.Type)
                        .Set("asset_flags", Convert.ToInt32(meta.Flags));
                    collection.UpdateOne(filter, update);
                }

                return found;

            }
            catch (Exception e)
            {
                m_log.Error("[MongoDB FSASSETS] Failed to store asset with ID " + meta.ID);
                m_log.Error(e.ToString());
                return false;
            }
        }


        /// <summary>
        /// Check if the assets exist in the database.
        /// </summary>
        /// <param name="uuids">The asset UUID's</param>
        /// <returns>For each asset: true if it exists, false otherwise</returns>
        public bool[] AssetsExist(UUID[] uuids)
        {
            if (uuids.Length == 0)
                return new bool[0];

            // Converte os UUIDs para string e cria uma lista de filtros para a consulta
            var uuidStrings = uuids.Select(uuid => uuid.ToString()).ToList();
            var filter = Builders<BsonDocument>.Filter.In("id", uuidStrings);

            // Executa a consulta no MongoDB
            var collection = _db.GetCollection<BsonDocument>(m_Table);
            var results = collection.Find(filter).ToList();

            // Constrói o conjunto de UUIDs encontrados
            HashSet<UUID> exists = new HashSet<UUID>();
            foreach (var doc in results)
            {
                exists.Add(new UUID(doc["id"].AsString));
            }

            // Preenche o array de retorno com `true` ou `false` conforme o UUID existir no conjunto
            bool[] output = new bool[uuids.Length];
            for (int i = 0; i < uuids.Length; i++)
            {
                output[i] = exists.Contains(uuids[i]);
            }

            return output;
        }

        public int Count()
        {
            var collection = _db.GetCollection<BsonDocument>(m_Table);
            return (int)collection.CountDocuments(new BsonDocument());
        }

        public bool Delete(string id)
        {
            var collection = _db.GetCollection<BsonDocument>(m_Table);
            var filter = Builders<BsonDocument>.Filter.Eq("_id", new ObjectId(id));
            var result = collection.DeleteOne(filter);
            return result.DeletedCount > 0;
        }

        public void Import(string conn, string table, int start, int count, bool force, FSStoreDelegate store)
        {
            int imported = 0;
            string limit = String.Empty;
            if(count != -1)
            {
                limit = String.Format(" limit {0} offset {1}", start, count);
            }
            /*
            string query = String.Format("select * from {0}{1}", table, limit);
            try
            {
                using (NpgsqlConnection remote = new NpgsqlConnection(conn))
                using (NpgsqlCommand cmd = new NpgsqlCommand(query, remote))
                {
                    remote.Open();
                    MainConsole.Instance.Output("Querying database");
                    MainConsole.Instance.Output("Reading data");
                    using (NpgsqlDataReader reader = cmd.ExecuteReader(CommandBehavior.Default))
                    {
                        while (reader.Read())
                        {
                            if ((imported % 100) == 0)
                            {
                                MainConsole.Instance.Output(String.Format("{0} assets imported so far", imported));
                            }
    
                            AssetBase asset = new AssetBase();
                            AssetMetadata meta = new AssetMetadata();

                            meta.ID = reader["id"].ToString();
                            meta.FullID = new UUID(meta.ID);

                            meta.Name = String.Empty;
                            meta.Description = String.Empty;
                            meta.Type = (sbyte)Convert.ToInt32(reader["assetType"]);
                            meta.ContentType = SLUtil.SLAssetTypeToContentType(meta.Type);
                            meta.CreationDate = Util.ToDateTime(Convert.ToInt32(reader["create_time"]));

                            asset.Metadata = meta;
                            asset.Data = (byte[])reader["data"];

                            store(asset, force);

                            imported++;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PGSQL FSASSETS]: Error importing assets: {0}",
                        e.Message.ToString());
                return;
            }*/

            MainConsole.Instance.Output(String.Format("Import done, {0} assets imported", imported));
        }

        #endregion
    }
}
