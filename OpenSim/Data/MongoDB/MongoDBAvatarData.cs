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
using System.Threading;
using log4net;
using MongoDB.Driver;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Services.Interfaces;


namespace OpenSim.Data.MongoDB
{
    /// <summary>
    /// A PGSQL Interface for Avatar Storage
    /// </summary>
    public class MongoDBAvatarData : MongoDBGenericTableHandler<AvatarBaseData>,
            IAvatarData
    {
//        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public MongoDBAvatarData(string connectionString, string realm) :
                base(connectionString, realm, "Avatar")
        {
        }

        public bool Delete(UUID principalID, string name)
        {
            var mongoClient = new MongoClient(m_ConnectionString);
            var db = mongoClient.GetDatabase(m_database.GetDatabaseName());
            var collection = db.GetCollection<AvatarBaseData>(m_Realm); // Substitua 'YourDocumentType' pelo tipo de documento correto

            var filter = Builders<AvatarBaseData>.Filter.And(
                Builders<AvatarBaseData>.Filter.Eq(doc => doc.PrincipalID, principalID), // Assumindo que existe uma propriedade PrincipalID
                Builders<AvatarBaseData>.Filter.ElemMatch(doc => doc.Data,
                Builders<KeyValuePair<string, string>>.Filter.And(
                    Builders<KeyValuePair<string, string>>.Filter.Eq(kv => kv.Key, "name"), // Verifica a chave "name"
                    Builders<KeyValuePair<string, string>>.Filter.Eq(kv => kv.Value, name) // Verifica o valor correspondente
                ))
            );

            var result = collection.DeleteOne(filter); // Remove um único documento que corresponde ao filtro

            return result.DeletedCount > 0; // Retorna true se um documento foi deletado
        }
    }
}
