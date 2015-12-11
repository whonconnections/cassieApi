using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Web;
using System.Web.Http;
using cassieApi.Models;
using Cassandra;
using Cassandra.Mapping;
using Newtonsoft.Json;

namespace cassieApi.Controllers
{
    public class CasController : ApiController
    {
        private static string Users_Table = System.Configuration.ConfigurationManager.AppSettings["UserTable"];
        private static string Users_Keyspace = System.Configuration.ConfigurationManager.AppSettings["UserKeyspace"];
        private static IMapper _mapper;

        static CasController()
        {
            var cluster = Cluster.Builder()
              .AddContactPoints(System.Configuration.ConfigurationManager.
                    ConnectionStrings["Cassandra"].ConnectionString.Split(',').Select(x => x.Trim()))
              .Build();

            //Create connections to the nodes using a keyspace
            var session = cluster.Connect();
            session.Execute($"CREATE keyspace if not exists {Users_Keyspace} WITH replication" + " = {'class': 'SimpleStrategy', 'replication_factor' : 1}");
            session.Execute($@"CREATE TABLE IF NOT EXISTS {Users_Keyspace}.{Users_Table} (id uuid, username text, PRIMARY KEY(username)) ");

            session = cluster.Connect(Users_Keyspace);
            MappingConfiguration.Global.Define(
               new Map<User>()
                  .TableName(Users_Table)
                  .PartitionKey(u => u.Username)
                  .Column(u => u.Username, cm => cm.WithName("username"))
                  .Column(u => u.Id, cm => cm.WithName("id")));

            _mapper = new Mapper(session);

        }

        public IHttpActionResult Post([FromBody] User user)
        {
            string uri = string.Format(CultureInfo.CurrentCulture, "{0}/{1}", Request.RequestUri, user.Username);
            User nUser = new User {Id = Guid.NewGuid(), Username = user.Username};
            _mapper.Insert(nUser);

            using (var myHttpClient = new HttpClient())
            {
                string postBody = JsonConvert.SerializeObject(nUser);
                myHttpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var test = myHttpClient.PostAsync("https://localhost:44301/api/ats",
                    new StringContent(postBody, Encoding.UTF8, "application/json")).Result;
            }

            return Created(uri, nUser);
        }

        public IHttpActionResult Get()
        {
            IEnumerable<User> users = _mapper.Fetch<User>($"SELECT id, username FROM {Users_Table}");
            return Ok(users);
        }

    }
}