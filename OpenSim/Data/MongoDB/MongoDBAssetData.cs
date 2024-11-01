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
using System.Data;
using System.Reflection;
using System.Collections.Generic;
using OpenMetaverse;
using log4net;
using OpenSim.Framework;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Linq;

namespace OpenSim.Data.MongoDB
{
    /// <summary>
    /// A PGSQL Interface for the Asset server
    /// </summary>
    public class MongoDBAssetData : AssetDataBase
    {
        private const string _migrationStore = "AssetStore";

        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private long m_ticksToEpoch;
        /// <summary>
        /// Database manager
        /// </summary>
        private MongoDBManager m_database;
        private string m_connectionString;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        #region IPlugin Members

        override public void Dispose() { }

        /// <summary>
        /// <para>Initialises asset interface</para>
        /// </summary>
        // [Obsolete("Cannot be default-initialized!")]
        override public void Initialise()
        {
            m_log.Info("[MongoDBAssetData]: " + Name + " cannot be default-initialized!");
            throw new PluginNotInitialisedException(Name);
        }

        /// <summary>
        /// Initialises asset interface
        /// </summary>
        /// <para>
        /// a string instead of file, if someone writes the support
        /// </para>
        /// <param name="connectionString">connect string</param>
        override public void Initialise(string connectionString)
        {
            m_ticksToEpoch = new System.DateTime(1970, 1, 1).Ticks;

            m_database = new MongoDBManager(connectionString);
            m_connectionString = connectionString;

            //New migration to check for DB changes
            m_database.CheckMigration(_migrationStore);
        }

        /// <summary>
        /// Database provider version.
        /// </summary>
        override public string Version
        {
            get { return m_database.getVersion(); }
        }

        /// <summary>
        /// The name of this DB provider.
        /// </summary>
        override public string Name
        {
            get { return "MongoDB Asset storage engine"; }
        }

        #endregion

        #region IAssetDataPlugin Members

        /// <summary>
        /// Fetch Asset from m_database
        /// </summary>
        /// <param name="assetID">the asset UUID</param>
        /// <returns></returns>
        override public AssetBase GetAsset(UUID assetID)
        {
            var client = new MongoClient(m_connectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());
            var collection = database.GetCollection<BsonDocument>("assets");

            // Cria o filtro para encontrar o documento pelo ID do asset
            var filter = Builders<BsonDocument>.Filter.Eq("id", assetID.ToString());

            // Executa a busca pelo documento
            var document = collection.Find(filter).FirstOrDefault();

            if (document != null)
            {
                AssetBase asset = new AssetBase(
                    new UUID(document["id"].AsString),
                    document["name"].AsString,
                    (sbyte)document["assetType"].AsInt32,
                    document["creatorid"].AsString
                );

                // Region Main
                asset.Description = document["description"].AsString;
                asset.Local = document["local"].AsBoolean;
                asset.Temporary = document["temporary"].AsBoolean;
                asset.Flags = (AssetFlags)document["asset_flags"].AsInt32;
                asset.Data = document["data"].AsByteArray;

                return asset;
            }

            return null; // Nenhum documento encontrado
        }

        /// <summary>
        /// Create asset in m_database
        /// </summary>
        /// <param name="asset">the asset</param>
        override public bool StoreAsset(AssetBase asset)
        {
            var client = new MongoClient(m_connectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());
            var collection = database.GetCollection<BsonDocument>("assets");

            string assetName = asset.Name;
            if (asset.Name.Length > AssetBase.MAX_ASSET_NAME)
            {
                assetName = asset.Name.Substring(0, AssetBase.MAX_ASSET_NAME);
                m_log.WarnFormat(
                    "[ASSET DB]: Name '{0}' for asset {1} truncated from {2} to {3} characters on add",
                    asset.Name, asset.ID, asset.Name.Length, assetName.Length);
            }

            string assetDescription = asset.Description;
            if (asset.Description.Length > AssetBase.MAX_ASSET_DESC)
            {
                assetDescription = asset.Description.Substring(0, AssetBase.MAX_ASSET_DESC);
                m_log.WarnFormat(
                    "[ASSET DB]: Description '{0}' for asset {1} truncated from {2} to {3} characters on add",
                    asset.Description, asset.ID, asset.Description.Length, assetDescription.Length);
            }

            int now = (int)((DateTime.Now.Ticks - m_ticksToEpoch) / 10000000);

            var filter = Builders<BsonDocument>.Filter.Eq("id", asset.FullID.ToString());
            var update = Builders<BsonDocument>.Update
                .Set("name", assetName)
                .Set("description", assetDescription)
                .Set("assetType", asset.Type)
                .Set("local", asset.Local)
                .Set("temporary", asset.Temporary)
                .Set("creatorid", asset.Metadata.CreatorID)
                .Set("data", asset.Data)
                .Set("create_time", now)
                .Set("access_time", now)
                .Set("asset_flags", (int)asset.Flags);

            try
            {
                // Upsert: update if exists, insert if not
                var result = collection.UpdateOne(filter, update, new UpdateOptions { IsUpsert = true });
            }
            catch (Exception e)
            {
                m_log.Error("[ASSET DB]: Error storing item: " + e.Message);
                return false;
            }

            return true;
        }


// Commented out since currently unused - this probably should be called in GetAsset()
//        private void UpdateAccessTime(AssetBase asset)
//        {
//            using (AutoClosingSqlCommand cmd = m_database.Query("UPDATE assets SET access_time = :access_time WHERE id=:id"))
//            {
//                int now = (int)((System.DateTime.Now.Ticks - m_ticksToEpoch) / 10000000);
//                cmd.Parameters.AddWithValue(":id", asset.FullID.ToString());
//                cmd.Parameters.AddWithValue(":access_time", now);
//                try
//                {
//                    cmd.ExecuteNonQuery();
//                }
//                catch (Exception e)
//                {
//                    m_log.Error(e.ToString());
//                }
//            }
//        }

        /// <summary>
        /// Check if the assets exist in the database.
        /// </summary>
        /// <param name="uuids">The assets' IDs</param>
        /// <returns>For each asset: true if it exists, false otherwise</returns>
        public override bool[] AssetsExist(UUID[] uuids)
        {
            if (uuids.Length == 0)
                return new bool[0];

            var exist = new HashSet<UUID>();
            var client = new MongoClient(m_connectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());

            var db = client.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<BsonDocument>("assets");

            // Cria filtro para buscar documentos com ids especificados
            var filter = Builders<BsonDocument>.Filter.In("id", uuids.Select(u => u.ToString()));

            // Executa a consulta
            var documents = collection.Find(filter).ToEnumerable();

            // Adiciona os IDs encontrados ao HashSet
            foreach (var doc in documents)
            {
                UUID id = new UUID(doc["id"].AsString);
                exist.Add(id);
            }

            // Verifica se cada UUID da lista original existe
            bool[] results = new bool[uuids.Length];
            for (int i = 0; i < uuids.Length; i++)
                results[i] = exist.Contains(uuids[i]);

            return results;
        }

        /// <summary>
        /// Returns a list of AssetMetadata objects. The list is a subset of
        /// the entire data set offset by <paramref name="start" /> containing
        /// <paramref name="count" /> elements.
        /// </summary>
        /// <param name="start">The number of results to discard from the total data set.</param>
        /// <param name="count">The number of rows the returned list should contain.</param>
        /// <returns>A list of AssetMetadata objects.</returns>
        public override List<AssetMetadata> FetchAssetMetadataSet(int start, int count)
        {
            var retList = new List<AssetMetadata>(count);
            var client = new MongoClient(m_connectionString);
            var database = client.GetDatabase(m_database.GetDatabaseName());
            var db = client.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<BsonDocument>("assets");

            // Definindo a consulta e a paginação
            var filter = Builders<BsonDocument>.Filter.Empty;
            var sort = Builders<BsonDocument>.Sort.Ascending("id");

            // Executa a consulta com ordenação, limite e offset
            var documents = collection.Find(filter)
                                      .Sort(sort)
                                      .Skip(start)
                                      .Limit(count)
                                      .ToEnumerable();

            foreach (var doc in documents)
            {
                var metadata = new AssetMetadata
                {
                    FullID = new UUID(doc["id"].AsString),
                    Name = doc["name"].AsString,
                    Description = doc["description"].AsString,
                    Type = Convert.ToSByte(doc["assetType"]),
                    Temporary = doc["temporary"].AsBoolean,
                    CreatorID = doc["creatorid"].AsString
                };
                retList.Add(metadata);
            }

            return retList;
        }

        public override bool Delete(string id)
        {
            return false;
        }
        #endregion
    }
}
