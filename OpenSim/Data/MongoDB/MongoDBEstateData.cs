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
using System.Reflection;
using log4net;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;
using System.Data;
using MongoDB.Bson;
using MongoDB.Driver;

namespace OpenSim.Data.MongoDB
{
    public class PGSQLEstateStore : IEstateDataStore
    {
        private const string _migrationStore = "EstateStore";

        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private MongoDBManager _Database;
        private string m_connectionString;
        private FieldInfo[] _Fields;
        private Dictionary<string, FieldInfo> _FieldMap = new Dictionary<string, FieldInfo>();

        private MongoClient _mongoClient;
        private IMongoDatabase _db;

        #region Public methods

        public PGSQLEstateStore()
        {
        }

        public PGSQLEstateStore(string connectionString)
        {
            Initialise(connectionString);
        }

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        /// <summary>
        /// Initialises the estatedata class.
        /// </summary>
        /// <param name="connectionString">connectionString.</param>
        public void Initialise(string connectionString)
        {
            if (!string.IsNullOrEmpty(connectionString))
            {
                m_connectionString = connectionString;
                _Database = new MongoDBManager(connectionString);
            }

            _mongoClient = new MongoClient(m_connectionString);
            _db = _mongoClient.GetDatabase(_Database.GetDatabaseName());

            /*
            //Migration settings
            using (NpgsqlConnection conn = new NpgsqlConnection(m_connectionString))
            {
                conn.Open();
                Migration m = new Migration(conn, GetType().Assembly, "EstateStore");
                m.Update();
            }
            */

            //Interesting way to get parameters! Maybe implement that also with other types
            Type t = typeof(EstateSettings);
            _Fields = t.GetFields(BindingFlags.NonPublic |
                                  BindingFlags.Instance |
                                  BindingFlags.DeclaredOnly);

            foreach (FieldInfo f in _Fields)
            {
                if (f.Name.Substring(0, 2) == "m_")
                    _FieldMap[f.Name.Substring(2)] = f;
            }
        }

        /// <summary>
        /// Loads the estate settings.
        /// </summary>
        /// <param name="regionID">region ID.</param>
        /// <returns></returns>
        public EstateSettings LoadEstateSettings(UUID regionID, bool create)
        {
            EstateSettings es = new EstateSettings();

            // Obtenha a coleção de "estate_settings"
            var estateSettingsCollection = _db.GetCollection<BsonDocument>("estate_settings");
            var estateMapCollection = _db.GetCollection<BsonDocument>("estate_map");

            // Filtro para procurar o EstateSettings pelo RegionID na coleção estate_map
            var filter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("RegionID", regionID),
                Builders<BsonDocument>.Filter.Ne("EstateID", BsonNull.Value)
            );

            bool insertEstate = false;
            var estateDoc = estateMapCollection.Find(filter).FirstOrDefault();

            if (estateDoc != null)
            {
                // Itera por cada campo na lista FieldList e define os valores
                foreach (string name in FieldList)
                {
                    FieldInfo f = _FieldMap[name];
                    BsonValue v = estateDoc.Contains(name) ? estateDoc[name] : BsonNull.Value;

                    if (!v.IsBsonNull)
                    {
                        if (f.FieldType == typeof(bool))
                        {
                            f.SetValue(es, v.AsBoolean);
                        }
                        else if (f.FieldType == typeof(UUID))
                        {
                            UUID estUUID;
                            UUID.TryParse(v.AsString, out estUUID);
                            f.SetValue(es, estUUID);
                        }
                        else if (f.FieldType == typeof(string))
                        {
                            f.SetValue(es, v.AsString);
                        }
                        else if (f.FieldType == typeof(UInt32))
                        {
                            f.SetValue(es, Convert.ToUInt32(v.AsInt32));
                        }
                        else if (f.FieldType == typeof(Single))
                        {
                            f.SetValue(es, Convert.ToSingle(v.AsDouble));
                        }
                        else
                        {
                            f.SetValue(es, v);
                        }
                    }
                }
            }
            else
            {
                insertEstate = true;
            }

            // Insere uma nova entrada de estate se necessário
            if (insertEstate && create)
            {
                DoCreate(es);
                LinkRegion(regionID, (int)es.EstateID);
            }

            // Carrega listas associadas
            LoadBanList(es);
            es.EstateManagers = LoadUUIDList(es.EstateID, "estate_managers");
            es.EstateAccess = LoadUUIDList(es.EstateID, "estate_users");
            es.EstateGroups = LoadUUIDList(es.EstateID, "estate_groups");

            // Evento para salvar as configurações da propriedade
            es.OnSave += StoreEstateSettings;
            return es;
        }

        public EstateSettings CreateNewEstate(int estateID)
        {
            EstateSettings es = new EstateSettings();

            es.OnSave += StoreEstateSettings;
            es.EstateID = Convert.ToUInt32(estateID);

            DoCreate(es);

            LoadBanList(es);

            es.EstateManagers = LoadUUIDList(es.EstateID, "estate_managers");
            es.EstateAccess = LoadUUIDList(es.EstateID, "estate_users");
            es.EstateGroups = LoadUUIDList(es.EstateID, "estate_groups");

            return es;
        }

        private void DoCreate(EstateSettings es)
        {
            // Obtenha a lista de campos para inserir
            List<string> names = new List<string>(FieldList);

            // Simule um AutoIncrement se EstateID for menor que 100
            if (es.EstateID < 100)
            {
                es.EstateID = GetNextEstateID(); // Função auxiliar para gerar ID
                names.Remove("EstateID");
            }

            // Construa o documento para inserção
            var estateDoc = new BsonDocument();
            foreach (string name in names)
            {
                FieldInfo f = _FieldMap[name];
                estateDoc[name] = BsonValue.Create(f.GetValue(es));
            }

            estateDoc["EstateID"] = es.EstateID;

            // Insere o documento na coleção estate_settings
            var estateSettingsCollection = _db.GetCollection<BsonDocument>("estate_settings");

            estateSettingsCollection.InsertOne(estateDoc);

            // Chama Save para persistir o estado (caso necessário)
            es.Save();
        }

        // Função auxiliar para gerar o próximo EstateID incrementado
        private uint GetNextEstateID()
        {
            var counterCollection = _db.GetCollection<BsonDocument>("counters");

            // Encontra e incrementa o contador
            var filter = Builders<BsonDocument>.Filter.Eq("_id", "estateID");
            var update = Builders<BsonDocument>.Update.Inc("seq", 1);
            var options = new FindOneAndUpdateOptions<BsonDocument>
            {
                ReturnDocument = ReturnDocument.After,
                IsUpsert = true
            };

            var result = counterCollection.FindOneAndUpdate(filter, update, options);
            return (uint)result["seq"].AsInt32;
        }

        /// <summary>
        /// Stores the estate settings.
        /// </summary>
        /// <param name="es">estate settings</param>
        public void StoreEstateSettings(EstateSettings es)
        {
            // Constrói o documento de atualização excluindo o campo EstateID
            List<string> names = new List<string>(FieldList);
            names.Remove("EstateID");

            var updateDefinition = new BsonDocument();
            foreach (string name in names)
            {
                FieldInfo f = _FieldMap[name];
                updateDefinition[name] = BsonValue.Create(f.GetValue(es));
            }

            // Conecta ao MongoDB e atualiza o documento onde EstateID corresponde
            var estateSettingsCollection = _db.GetCollection<BsonDocument>("estate_settings");

            var filter = Builders<BsonDocument>.Filter.Eq("EstateID", es.EstateID);
            var update = new BsonDocument("$set", updateDefinition);

            estateSettingsCollection.UpdateOne(filter, update);

            // Atualiza listas relacionadas
            SaveBanList(es);
            SaveUUIDList(es.EstateID, "estate_managers", es.EstateManagers);
            SaveUUIDList(es.EstateID, "estate_users", es.EstateAccess);
            SaveUUIDList(es.EstateID, "estate_groups", es.EstateGroups);
        }

        #endregion

        #region Private methods

        private string[] FieldList
        {
            get { return new List<string>(_FieldMap.Keys).ToArray(); }
        }

        private void LoadBanList(EstateSettings es)
        {
            // Limpa a lista de bans antes de carregar os novos
            es.ClearBans();

            var estateBanCollection = _db.GetCollection<BsonDocument>("estateban");

            // Define o filtro para buscar apenas bans com o EstateID correspondente
            var filter = Builders<BsonDocument>.Filter.Eq("EstateID", (int)es.EstateID);

            // Executa a consulta e itera pelos resultados
            var bans = estateBanCollection.Find(filter).ToList();
            foreach (var banDoc in bans)
            {
                EstateBan eb = new EstateBan
                {
                    BannedUserID = new UUID(banDoc["bannedUUID"].AsGuid),
                    BanningUserID = new UUID(banDoc["banningUUID"].AsGuid),
                    BanTime = banDoc["banTime"].AsInt32,
                    BannedHostAddress = "0.0.0.0",
                    BannedHostIPMask = "0.0.0.0"
                };
                es.AddBan(eb);
            }
        }

        private UUID[] LoadUUIDList(uint estateID, string table)
        {
            List<UUID> uuids = new List<UUID>();

            var collection = _db.GetCollection<BsonDocument>(table);

            // Define o filtro para buscar UUIDs com o EstateID correspondente
            var filter = Builders<BsonDocument>.Filter.Eq("EstateID", (int)estateID);

            // Executa a consulta e itera pelos resultados
            var documents = collection.Find(filter).ToList();
            foreach (var doc in documents)
            {
                // Adiciona o UUID à lista
                uuids.Add(new UUID(doc["uuid"].AsGuid));
            }

            return uuids.ToArray();
        }

        private void SaveBanList(EstateSettings es)
        {
            var collection = _db.GetCollection<BsonDocument>("estateban");

            // Delete first
            var deleteFilter = Builders<BsonDocument>.Filter.Eq("EstateID", (int)es.EstateID);
            collection.DeleteMany(deleteFilter);

            // Insert after
            foreach (EstateBan b in es.EstateBans)
            {
                var ban = new BsonDocument
                {
                    { "EstateID", b.EstateID },
                    { "bannedUUID", b.BannedUserID.ToString() },
                    { "bannedIpHostMask", b.BannedHostIPMask },
                    { "bannedIp", b.BannedHostAddress },
                    { "banTime", b.BanTime }
                };
                collection.InsertOne(ban);
            }
        }

        private void SaveUUIDList(uint estateID, string table, UUID[] data)
        {
            var collection = _db.GetCollection<BsonDocument>(table);

            // Remove todos os documentos associados ao EstateID
            var filter = Builders<BsonDocument>.Filter.Eq("EstateID", estateID);
            collection.DeleteMany(filter);

            // Inserir novos documentos com EstateID e UUID
            var documents = new List<BsonDocument>();
            foreach (UUID uuid in data)
            {
                var document = new BsonDocument
                {
                    { "EstateID", estateID },
                    { "uuid", uuid.ToString() }  // Usando ToString() para armazenar como string
                };
                documents.Add(document);
            }

            if (documents.Count > 0)
            {
                collection.InsertMany(documents);
            }
        }

        public EstateSettings LoadEstateSettings(int estateID)
        {
            EstateSettings es = new EstateSettings();

            var collection = _db.GetCollection<BsonDocument>("estate_settings");
            var filter = Builders<BsonDocument>.Filter.Eq("EstateID", estateID);

            var document = collection.Find(filter).FirstOrDefault();
            if (document != null)
            {
                foreach (string name in FieldList)
                {
                    FieldInfo f = _FieldMap[name];
                    if (document.Contains(name))
                    {
                        var value = document[name];
                        if (f.FieldType == typeof(bool))
                        {
                            f.SetValue(es, value.ToInt32() != 0);
                        }
                        else if (f.FieldType == typeof(UUID))
                        {
                            f.SetValue(es, new UUID(Guid.Parse(value.AsString)));
                        }
                        else if (f.FieldType == typeof(string))
                        {
                            f.SetValue(es, value.AsString);
                        }
                        else if (f.FieldType == typeof(UInt32))
                        {
                            f.SetValue(es, Convert.ToUInt32(value.AsInt32));
                        }
                        else if (f.FieldType == typeof(Single))
                        {
                            f.SetValue(es, Convert.ToSingle(value.AsDouble));
                        }
                        else
                        {
                            f.SetValue(es, BsonTypeMapper.MapToDotNetValue(value));
                        }
                    }
                }
            }

            LoadBanList(es);

            es.EstateManagers = LoadUUIDList(es.EstateID, "estate_managers");
            es.EstateAccess = LoadUUIDList(es.EstateID, "estate_users");
            es.EstateGroups = LoadUUIDList(es.EstateID, "estate_groups");

            //Set event
            es.OnSave += StoreEstateSettings;
            return es;
        }

        public List<EstateSettings> LoadEstateSettingsAll()
        {
            List<EstateSettings> allEstateSettings = new List<EstateSettings>();

            List<int> allEstateIds = GetEstatesAll();

            foreach (int estateId in allEstateIds)
                allEstateSettings.Add(LoadEstateSettings(estateId));

            return allEstateSettings;
        }

        public List<int> GetEstates(string search)
        {
            List<int> result = new List<int>();

            var collection = _db.GetCollection<BsonDocument>("estate_settings");
            var filter = Builders<BsonDocument>.Filter.Eq("EstateName", search.ToLower());

            var documents = collection.Find(filter).ToList();

            foreach (var document in documents)
            {
                result.Add(document["EstateID"].AsInt32);
            }

            return result;
        }

        public List<int> GetEstatesAll()
        {
            List<int> result = new List<int>();

            var collection = _db.GetCollection<BsonDocument>("estate_settings");

            var documents = collection.Find(new BsonDocument()).ToList();

            foreach (var document in documents)
            {
                result.Add(document["EstateID"].AsInt32);
            }

            return result;
        }

        public List<int> GetEstatesByOwner(UUID ownerID)
        {
            List<int> result = new List<int>();

            var collection = _db.GetCollection<BsonDocument>("estate_settings");

            var filter = Builders<BsonDocument>.Filter.Eq("EstateOwner", ownerID.ToString()); // Converte o UUID para string

            var documents = collection.Find(filter).ToList();

            foreach (var document in documents)
            {
                result.Add(document["EstateID"].AsInt32);
            }

            return result;
        }

        public bool LinkRegion(UUID regionID, int estateID)
        {
            var collection = _db.GetCollection<BsonDocument>("estate_map");

            // Deleta o registro anterior
            var deleteFilter = Builders<BsonDocument>.Filter.Eq("RegionID", regionID.ToString()); // Converte o UUID para string
            var deleteResult = collection.DeleteMany(deleteFilter);

            // Cria um novo documento para inserir
            var newEntry = new BsonDocument
            {
                { "RegionID", regionID.ToString() }, // Converte o UUID para string
                { "EstateID", estateID }
            };

            try
            {
                // Insere o novo documento
                collection.InsertOne(newEntry);
                return true; // Se a inserção for bem-sucedida, retorna true
            }
            catch (MongoWriteException ex) // Captura exceções relacionadas a gravações
            {
                m_log.Error("[REGION DB]: LinkRegion failed: " + ex.Message);
                return false; // Retorna false se ocorrer uma exceção
            }
            catch (Exception ex) // Captura outras exceções
            {
                m_log.Error("[REGION DB]: LinkRegion failed: " + ex.Message);
                return false; // Retorna false se ocorrer uma exceção
            }
        }

        public List<UUID> GetRegions(int estateID)
        {
            List<UUID> result = new List<UUID>();

            var collection = _db.GetCollection<BsonDocument>("estate_map");

            var filter = Builders<BsonDocument>.Filter.Eq("EstateID", estateID);
            var regions = collection.Find(filter).ToList();

            foreach (var region in regions)
            {
                // Converte o RegionID de string para UUID
                result.Add(DBGuid.FromDB(region["RegionID"].AsString)); // Assume que RegionID é armazenado como string
            }

            return result;
        }

        public bool DeleteEstate(int estateID)
        {
            // TODO: Implementation!
            return false;
        }
        #endregion
    }
}
